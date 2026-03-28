"""
Signal: News Sentiment
Uses Finnhub to detect when recent news creates a directional edge in a market.

Logic:
  1. Fetch general market news headlines (cached 30 min).
  2. For each relevant market, attempt to extract a stock ticker from the question.
     - If found: fetch company sentiment from Finnhub.
     - Bullish signal: sentiment > 0.2 AND buzz > 0.4 AND articles >= 3
     - Bearish signal: sentiment < -0.2 AND buzz > 0.4 AND articles >= 3
  3. Even without a ticker, scan headlines for mentions matching the market topic:
     - 3+ matching positive headlines → bullish signal
     - 3+ matching negative headlines → bearish signal

Filtered out:
  - Markets with liquidity < 500
  - Pure sports/soccer markets (news sentiment doesn't help)
  - Markets whose category/question doesn't map to RELEVANT_CATEGORIES or contain a ticker

Uses signal_type = MISPRICED_CORR.
"""
import re
import json

from src.enrichers.finnhub import FinnhubClient
from src.signals.base import BaseSignal, Opportunity, SignalType
from src.utils.logger import get_logger

logger = get_logger(__name__)

RELEVANT_CATEGORIES = {"politics", "finance", "crypto", "business", "tech", "election", "economy", "stock"}

SPORTS_KEYWORDS = [
    "soccer", "football", "basketball", "baseball",
    "hockey", "tennis", "golf", "mma", "ufc",
]


class NewsSentimentSignal(BaseSignal):
    """
    Detects mispricings driven by recent news sentiment that the market
    has not yet fully reflected.
    """

    def __init__(self, finnhub_client: FinnhubClient, fee_rate: float = 0.02, min_edge: float = 0.03):
        super().__init__(fee_rate=fee_rate, min_edge=min_edge)
        self.finnhub_client = finnhub_client

    @property
    def name(self) -> str:
        return "NEWS_SENTIMENT"

    # ── Main entry point ───────────────────────────────────────────────────────

    def detect(self, markets: list[dict], prices: dict = None) -> list[Opportunity]:
        opportunities = []

        # Step 1: fetch general news headlines once (cached)
        headlines = self.finnhub_client.get_market_news_sentiment(category="general")

        for market in markets:
            try:
                question = market.get("question", "")
                if not question:
                    continue

                category = self._get_category(market)

                # Skip irrelevant or sports markets
                if not self._is_relevant(question, category):
                    continue

                # Skip low-liquidity markets
                liquidity = market.get("liquidityNum") or 0
                if liquidity < 500:
                    continue

                # Resolve YES price
                outcome_prices_raw = market.get("outcomePrices")
                if not outcome_prices_raw:
                    continue
                outcome_prices = (
                    json.loads(outcome_prices_raw)
                    if isinstance(outcome_prices_raw, str)
                    else outcome_prices_raw
                )
                if len(outcome_prices) < 2:
                    continue

                yes_price = float(outcome_prices[0])
                if yes_price <= 0 or yes_price >= 0.98:
                    continue

                token_ids = (
                    json.loads(market.get("clobTokenIds", "[]"))
                    if isinstance(market.get("clobTokenIds"), str)
                    else market.get("clobTokenIds") or []
                )

                # Step 2a: ticker-based company sentiment
                opp = self._evaluate_ticker_signal(market, question, category, yes_price, token_ids)

                # Step 2b: fallback — headline keyword matching
                if opp is None:
                    opp = self._evaluate_headline_signal(market, question, category, yes_price, token_ids, headlines)

                if opp is not None:
                    opportunities.append(opp)

            except Exception as e:
                logger.debug(f"NewsSentiment skip {market.get('conditionId', '?')[:12]}: {e}")

        opportunities.sort(key=lambda o: o.edge, reverse=True)
        return opportunities

    # ── Ticker-based evaluation ────────────────────────────────────────────────

    def _evaluate_ticker_signal(
        self,
        market: dict,
        question: str,
        category: str,
        yes_price: float,
        token_ids: list,
    ) -> Opportunity | None:
        ticker = self.finnhub_client.extract_ticker_from_question(question)
        if not ticker:
            return None

        sentiment_data = self.finnhub_client.get_company_sentiment(ticker)
        sentiment = sentiment_data["sentiment"]
        buzz = sentiment_data["buzz"]
        articles = sentiment_data["articles_last_week"]

        is_bullish = sentiment > 0.2 and buzz > 0.4 and articles >= self.finnhub_client.MIN_SENTIMENT_ARTICLES
        is_bearish = sentiment < -0.2 and buzz > 0.4 and articles >= self.finnhub_client.MIN_SENTIMENT_ARTICLES

        if not is_bullish and not is_bearish:
            return None

        notes_prefix = (
            f"Finnhub ${ticker}: sentiment={sentiment:.3f} buzz={buzz:.2f} "
            f"articles={articles}"
        )
        return self._build_opportunity(
            market=market,
            question=question,
            category=category,
            yes_price=yes_price,
            token_ids=token_ids,
            bullish=is_bullish,
            notes_prefix=notes_prefix,
        )

    # ── Headline-matching evaluation ───────────────────────────────────────────

    def _evaluate_headline_signal(
        self,
        market: dict,
        question: str,
        category: str,
        yes_price: float,
        token_ids: list,
        headlines: list[dict],
    ) -> Opportunity | None:
        positive_count = 0
        negative_count = 0

        # Build a set of meaningful words from the question to match against headlines
        question_words = set(re.findall(r'[a-z]{4,}', question.lower()))
        # Remove very common words that would produce false positives
        stopwords = {"will", "that", "this", "with", "from", "have", "been", "what", "when", "than", "more", "over"}
        question_words -= stopwords

        if len(question_words) < 2:
            return None

        for item in headlines:
            headline = item.get("headline", "").lower()
            sentiment = item.get("sentiment", "neutral")

            # Require at least 2 question words to appear in the headline
            matches = sum(1 for w in question_words if w in headline)
            if matches < 2:
                continue

            if sentiment == "positive":
                positive_count += 1
            elif sentiment == "negative":
                negative_count += 1

        is_bullish = positive_count >= 3
        is_bearish = negative_count >= 3

        if not is_bullish and not is_bearish:
            return None

        direction = "bullish" if is_bullish else "bearish"
        count = positive_count if is_bullish else negative_count
        notes_prefix = f"Headlines: {count} {direction} matches for \"{question[:40]}\""

        return self._build_opportunity(
            market=market,
            question=question,
            category=category,
            yes_price=yes_price,
            token_ids=token_ids,
            bullish=is_bullish,
            notes_prefix=notes_prefix,
        )

    # ── Shared opportunity builder ─────────────────────────────────────────────

    def _build_opportunity(
        self,
        market: dict,
        question: str,
        category: str,
        yes_price: float,
        token_ids: list,
        bullish: bool,
        notes_prefix: str,
    ) -> Opportunity | None:
        if bullish:
            fair_value = min(yes_price + 0.08, 0.97)
            edge = fair_value - yes_price - self.fee_rate
            side = "YES"
            market_price = yes_price
            token_id = token_ids[0] if token_ids else ""
        else:
            no_price = 1.0 - yes_price
            fair_no = min(no_price + 0.08, 0.97)
            edge = fair_no - no_price - self.fee_rate
            side = "NO"
            market_price = no_price
            fair_value = fair_no
            token_id = token_ids[1] if len(token_ids) > 1 else ""

        if edge < self.min_edge:
            return None

        edge_pct = round(edge / market_price, 4) if market_price > 0 else 0

        opp = Opportunity(
            signal_type=SignalType.MISPRICED_CORR,
            condition_id=market.get("conditionId", ""),
            question=question,
            category=category,
            token_id=token_id,
            side=side,
            market_price=round(market_price, 5),
            fair_value=round(fair_value, 5),
            edge=round(edge, 5),
            edge_pct=edge_pct,
            best_bid=market.get("bestBid") or 0,
            best_ask=market.get("bestAsk") or 0,
            spread=market.get("spread") or 0,
            liquidity_usd=market.get("liquidityNum") or 0,
            volume_24h=market.get("volume24hr") or 0,
            notes=(
                f"{notes_prefix} | "
                f"poly={market_price:.3f} fair={fair_value:.3f} edge={edge:.4f}"
            ),
        )
        logger.info(
            f"[NEWS_SENTIMENT] {question[:50]} | side={side} "
            f"poly={market_price:.3f} fair={fair_value:.3f} edge={edge:.4f}"
        )
        return opp

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _is_relevant(self, question: str, category: str) -> bool:
        """
        Returns True if the market is relevant for news-driven analysis.
        Excludes pure sports markets; requires a known finance/politics/tech
        category OR an explicit $TICKER in the question.
        """
        text = f"{question} {category}".lower()

        if any(s in text for s in SPORTS_KEYWORDS):
            return False

        return (
            any(c in text for c in RELEVANT_CATEGORIES)
            or bool(re.search(r'\$[A-Z]{1,5}\b', question))
        )

    @staticmethod
    def _get_category(market: dict) -> str:
        events = market.get("events")
        if events and isinstance(events, list) and events:
            return events[0].get("category", "") or events[0].get("title", "")
        return market.get("category", "")
