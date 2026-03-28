"""
Enricher: Finnhub
https://finnhub.io/docs/api/company-news

Fetches news sentiment scores from the Finnhub free tier.
Free tier: 60 API calls/minute, no monthly limit.

Provides:
  - Company-level sentiment scores (/news-sentiment)
  - General market news headlines with keyword-based sentiment (/news)
"""
import re
import time
import requests
from src.utils.logger import get_logger

logger = get_logger(__name__)

BASE_URL = "https://finnhub.io/api/v1"

POSITIVE_KEYWORDS = ["wins", "beats", "surges", "rally", "jumps", "record", "gains"]
NEGATIVE_KEYWORDS = ["loses", "falls", "drops", "crash", "decline", "plunge", "fails"]


class FinnhubClient:
    """
    Client for the Finnhub REST API (free tier).

    Caches responses locally to stay within rate limits:
      - Company sentiment: 30 minutes
      - Market news: 30 minutes
    """

    CACHE_TTL_SECS = 1800       # 30 minutes
    MIN_SENTIMENT_ARTICLES = 3  # minimum articles for a reliable signal

    def __init__(self, api_key: str, timeout: int = 10):
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()
        # cache structure: { key: (timestamp, payload) }
        self._cache: dict[str, tuple[float, any]] = {}

    # ── Cache helpers ──────────────────────────────────────────────────────────

    def _cache_get(self, key: str):
        entry = self._cache.get(key)
        if entry is None:
            return None
        ts, payload = entry
        if time.time() - ts < self.CACHE_TTL_SECS:
            return payload
        return None

    def _cache_set(self, key: str, payload) -> None:
        self._cache[key] = (time.time(), payload)

    # ── API calls ──────────────────────────────────────────────────────────────

    def get_company_sentiment(self, symbol: str) -> dict:
        """
        Fetches /news-sentiment for a given stock symbol.

        Returns:
            {
                "buzz":               float,  # 0-1, higher = more news volume
                "sentiment":          float,  # -1 to 1, positive = bullish
                "articles_last_week": int,
            }
        On any error returns safe defaults (neutral sentiment, zero buzz).
        """
        cache_key = f"sentiment:{symbol.upper()}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        try:
            resp = self.session.get(
                f"{BASE_URL}/news-sentiment",
                params={"symbol": symbol.upper(), "token": self.api_key},
                timeout=self.timeout,
            )
            resp.raise_for_status()
            raw = resp.json()

            result = {
                "buzz": float(raw.get("buzz", {}).get("buzz", 0.5)),
                "sentiment": float(raw.get("sentiment", {}).get("bearishPercent", 0.5) * -1
                                   + raw.get("sentiment", {}).get("bullishPercent", 0.5)),
                "articles_last_week": int(raw.get("buzz", {}).get("articlesInLastWeek", 0)),
            }
            # Finnhub returns companyNewsScore as a direct sentiment proxy too
            # Prefer it when available: range 0-1 (0=bearish, 1=bullish) → remap to -1..1
            company_score = raw.get("companyNewsScore")
            if company_score is not None:
                result["sentiment"] = round(float(company_score) * 2 - 1, 4)

            self._cache_set(cache_key, result)
            logger.debug(f"Finnhub sentiment {symbol}: {result}")
            return result

        except Exception as e:
            logger.warning(f"Finnhub company sentiment failed for {symbol}: {e}")
            return {"buzz": 0.5, "sentiment": 0.0, "articles_last_week": 0}

    def get_market_news_sentiment(self, category: str = "general") -> list[dict]:
        """
        Fetches /news for the given category and annotates each headline with a
        simple keyword-based sentiment field.

        category: "general" | "forex" | "crypto" | "merger"

        Returns list of:
            {
                "headline":  str,
                "sentiment": str,   # "positive" | "negative" | "neutral"
            }
        """
        cache_key = f"news:{category}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        try:
            resp = self.session.get(
                f"{BASE_URL}/news",
                params={"category": category, "token": self.api_key},
                timeout=self.timeout,
            )
            resp.raise_for_status()
            articles = resp.json()

            result = []
            for article in articles:
                headline = article.get("headline", "")
                result.append({
                    "headline": headline,
                    "sentiment": self._keyword_sentiment(headline),
                })

            self._cache_set(cache_key, result)
            logger.debug(f"Finnhub market news ({category}): {len(result)} headlines fetched")
            return result

        except Exception as e:
            logger.warning(f"Finnhub market news failed (category={category}): {e}")
            return []

    # ── Utility ────────────────────────────────────────────────────────────────

    def extract_ticker_from_question(self, question: str) -> str | None:
        """
        Attempts to extract a stock ticker from a Polymarket question.

        Handles explicit $TICKER notation, e.g. "Will $AAPL exceed $200?"
        Returns the ticker string (uppercase, no $) or None.
        """
        m = re.search(r'\$([A-Z]{1,5})\b', question)
        if m:
            return m.group(1)
        return None

    @staticmethod
    def _keyword_sentiment(headline: str) -> str:
        """
        Classifies a headline as 'positive', 'negative', or 'neutral'
        using a simple keyword lookup against the headline text (case-insensitive).
        """
        text = headline.lower()
        if any(kw in text for kw in POSITIVE_KEYWORDS):
            return "positive"
        if any(kw in text for kw in NEGATIVE_KEYWORDS):
            return "negative"
        return "neutral"
