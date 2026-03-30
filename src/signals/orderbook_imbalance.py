"""
OrderBook Imbalance Signal

Detects when the CLOB bid/ask depth ratio strongly favors one side,
indicating whale buy or sell pressure that may push price toward fair value.
"""
import json
import time

from src.api.clob_client import ClobApiClient
from src.signals.base import BaseSignal, Opportunity, SignalType
from src.utils.logger import get_logger

logger = get_logger(__name__)

CACHE_TTL = 30  # seconds


class OrderBookImbalanceSignal(BaseSignal):
    """
    Fetches live CLOB orderbooks for top markets by liquidity and detects
    when bid/ask depth imbalance signals likely price movement.

    A ratio >= 0.70 (70%+ bid depth) suggests strong buy pressure -> long YES.
    A ratio <= 0.30 (30%- bid depth) suggests strong sell pressure -> long NO.
    """

    def __init__(
        self,
        clob_client: ClobApiClient,
        fee_rate: float = 0.02,
        min_edge: float = 0.03,
        max_per_scan: int = 20,
    ):
        super().__init__(fee_rate=fee_rate, min_edge=min_edge)
        self.clob_client = clob_client
        self.max_per_scan = max_per_scan
        self._book_cache: dict[str, tuple[float, dict]] = {}  # token_id -> (timestamp, orderbook)

    @property
    def name(self) -> str:
        return "orderbook_imbalance"

    def _get_category(self, market: dict) -> str:
        events = market.get("events")
        if events and isinstance(events, list) and events:
            return events[0].get("category", "") or events[0].get("title", "")
        return ""

    def _get_orderbook(self, token_id: str) -> dict | None:
        """Return orderbook from cache if fresh, otherwise fetch and cache."""
        now = time.monotonic()
        cached = self._book_cache.get(token_id)
        if cached is not None:
            ts, book = cached
            if now - ts < CACHE_TTL:
                return book

        try:
            book = self.clob_client.get_orderbook(token_id)
            self._book_cache[token_id] = (now, book)
            return book
        except Exception as exc:
            logger.debug("Failed to fetch orderbook for token %s: %s", token_id, exc)
            return None

    def _compute_depth(
        self, levels: list[dict], reference_price: float, side: str, window: float = 0.03
    ) -> float:
        """
        Sum sizes for levels within `window` cents of the reference price.

        For bids:  reference_price - window <= price <= reference_price
        For asks:  reference_price <= price <= reference_price + window
        """
        total = 0.0
        for level in levels:
            try:
                price = float(level["price"])
                size = float(level["size"])
            except (KeyError, ValueError, TypeError):
                continue

            if side == "bid":
                if reference_price - window <= price <= reference_price:
                    total += size
            else:  # ask
                if reference_price <= price <= reference_price + window:
                    total += size
        return total

    def detect(self, markets: list[dict], prices: dict = None) -> list[Opportunity]:
        opportunities: list[Opportunity] = []

        # Filter qualifying markets first
        qualifying = []
        for market in markets:
            liquidity = float(market.get("liquidityNum") or 0)
            volume = float(market.get("volume24hr") or 0)
            spread = float(market.get("spread") or 1.0)

            if liquidity < 1000 or volume < 200 or spread > 0.15:
                continue
            qualifying.append(market)

        # Sort by liquidity descending, take top N
        qualifying.sort(key=lambda m: float(m.get("liquidityNum") or 0), reverse=True)
        top_markets = qualifying[: self.max_per_scan]

        logger.debug(
            "%s: scanning %d/%d markets (max_per_scan=%d)",
            self.name,
            len(top_markets),
            len(markets),
            self.max_per_scan,
        )

        for market in top_markets:
            condition_id = market.get("conditionId", "")
            question = market.get("question", "")
            category = self._get_category(market)
            best_bid = float(market.get("bestBid") or 0)
            best_ask = float(market.get("bestAsk") or 0)
            spread = float(market.get("spread") or 1.0)
            liquidity_usd = float(market.get("liquidityNum") or 0)
            volume_24h = float(market.get("volume24hr") or 0)

            # Parse outcome prices
            outcome_prices_raw = market.get("outcomePrices", "[]")
            if isinstance(outcome_prices_raw, str):
                try:
                    outcome_prices = json.loads(outcome_prices_raw)
                except (json.JSONDecodeError, ValueError):
                    continue
            else:
                outcome_prices = outcome_prices_raw

            if not outcome_prices or len(outcome_prices) < 2:
                continue

            try:
                yes_price = float(outcome_prices[0])
            except (ValueError, TypeError):
                continue

            if yes_price <= 0 or yes_price >= 1:
                continue

            # Ignorar mercados fuera del rango 5%-95%: en longshots el bid imbalance
            # es estructural (siempre hay mas compradores que vendedores) y no es señal real.
            if yes_price < 0.05 or yes_price > 0.95:
                continue

            # Parse clobTokenIds
            clob_tokens_raw = market.get("clobTokenIds", "[]")
            if isinstance(clob_tokens_raw, str):
                try:
                    clob_tokens = json.loads(clob_tokens_raw)
                except (json.JSONDecodeError, ValueError):
                    continue
            else:
                clob_tokens = clob_tokens_raw

            if not clob_tokens:
                continue

            yes_token_id = str(clob_tokens[0])

            # Fetch (or retrieve cached) orderbook
            book = self._get_orderbook(yes_token_id)
            if book is None:
                continue

            bids = book.get("bids", [])
            asks = book.get("asks", [])

            if not bids and not asks:
                continue

            # Best bid/ask from orderbook (fall back to market fields)
            ob_best_bid = best_bid
            ob_best_ask = best_ask
            if bids:
                try:
                    ob_best_bid = max(float(b["price"]) for b in bids)
                except (KeyError, ValueError):
                    pass
            if asks:
                try:
                    ob_best_ask = min(float(a["price"]) for a in asks)
                except (KeyError, ValueError):
                    pass

            bid_depth = self._compute_depth(bids, ob_best_bid, side="bid")
            ask_depth = self._compute_depth(asks, ob_best_ask, side="ask")
            total_depth = bid_depth + ask_depth

            if total_depth <= 0:
                continue

            imbalance_ratio = bid_depth / total_depth

            logger.debug(
                "%s | %s | yes=%.3f bid_depth=%.0f ask_depth=%.0f ratio=%.3f",
                self.name,
                condition_id[:10],
                yes_price,
                bid_depth,
                ask_depth,
                imbalance_ratio,
            )

            # Strong buy pressure -> YES is underpriced
            if imbalance_ratio >= 0.70:
                fair_value = yes_price + (imbalance_ratio - 0.5) * 0.15
                edge = fair_value - yes_price - self.fee_rate

                if edge >= self.min_edge:
                    edge_pct = edge / yes_price if yes_price > 0 else 0.0
                    opp = Opportunity(
                        signal_type=SignalType.MISPRICED_CORR,
                        condition_id=condition_id,
                        question=question,
                        category=category,
                        token_id=yes_token_id,
                        side="YES",
                        market_price=yes_price,
                        fair_value=round(fair_value, 4),
                        edge=round(edge, 4),
                        edge_pct=round(edge_pct, 4),
                        best_bid=ob_best_bid,
                        best_ask=ob_best_ask,
                        spread=spread,
                        liquidity_usd=liquidity_usd,
                        volume_24h=volume_24h,
                        notes=(
                            f"Bid imbalance {imbalance_ratio:.1%}: "
                            f"bid_depth={bid_depth:.0f} ask_depth={ask_depth:.0f}"
                        ),
                    )
                    opportunities.append(opp)

            # Strong sell pressure -> NO is underpriced
            elif imbalance_ratio <= 0.30:
                no_price = 1.0 - yes_price
                fair_no = no_price + (0.5 - imbalance_ratio) * 0.15
                edge = fair_no - no_price - self.fee_rate

                if edge >= self.min_edge:
                    # NO token is at index 1
                    no_token_id = str(clob_tokens[1]) if len(clob_tokens) > 1 else yes_token_id
                    edge_pct = edge / no_price if no_price > 0 else 0.0
                    opp = Opportunity(
                        signal_type=SignalType.MISPRICED_CORR,
                        condition_id=condition_id,
                        question=question,
                        category=category,
                        token_id=no_token_id,
                        side="NO",
                        market_price=no_price,
                        fair_value=round(fair_no, 4),
                        edge=round(edge, 4),
                        edge_pct=round(edge_pct, 4),
                        best_bid=ob_best_bid,
                        best_ask=ob_best_ask,
                        spread=spread,
                        liquidity_usd=liquidity_usd,
                        volume_24h=volume_24h,
                        notes=(
                            f"Ask imbalance {1 - imbalance_ratio:.1%}: "
                            f"bid_depth={bid_depth:.0f} ask_depth={ask_depth:.0f}"
                        ),
                    )
                    opportunities.append(opp)

        opportunities.sort(key=lambda o: o.edge, reverse=True)
        logger.debug("%s: found %d opportunities", self.name, len(opportunities))
        return opportunities
