"""
Signal: Spread Capture + NO Farming
Gamma API incluye bestBid, bestAsk, spread directamente en el mercado.

Spread Capture: spread neto (spread - 2*fee) > min_edge => market making viable
NO Farming:     YES sobrevaluado por hype (YES_price > HYPE_THRESHOLD) y
                NO_price < fair_no - min_edge => comprar NO da edge positivo
"""
import json
from src.signals.base import BaseSignal, Opportunity, SignalType
from src.utils.logger import get_logger

logger = get_logger(__name__)

SPREAD_THRESHOLD   = 0.04   # spread minimo bruto para market making
MAX_SPREAD         = 0.25   # spread maximo — si es mayor es mercado zombi
HYPE_YES_THRESHOLD = 0.88   # YES sobrevaluado si precio > 88%
MIN_LIQUIDITY      = 500    # USD minimo de liquidez
MIN_VOLUME_24H     = 50     # USD minimo de volumen diario (sin esto nadie opera)


class SpreadSignal(BaseSignal):

    @property
    def name(self) -> str:
        return "SPREAD_CAPTURE"

    def detect(self, markets: list[dict], prices: dict = None) -> list[Opportunity]:
        opportunities = []

        for market in markets:
            try:
                liquidity  = market.get("liquidityNum") or 0
                volume_24h = market.get("volume24hr") or 0
                if liquidity < MIN_LIQUIDITY or volume_24h < MIN_VOLUME_24H:
                    continue

                best_bid = market.get("bestBid") or 0
                best_ask = market.get("bestAsk") or 0
                spread   = market.get("spread") or (best_ask - best_bid if best_ask > best_bid else 0)
                mid      = (best_bid + best_ask) / 2 if best_bid and best_ask else 0

                if best_bid <= 0 or best_ask <= 0:
                    continue

                token_ids = json.loads(market.get("clobTokenIds", "[]")) if isinstance(market.get("clobTokenIds"), str) else []
                category  = self._get_category(market)
                question  = market.get("question", "")
                cond_id   = market.get("conditionId", "")

                # ── Spread Capture ────────────────────────────────────────
                net_spread = spread - (2 * self.fee_rate)
                if net_spread >= self.min_edge and SPREAD_THRESHOLD <= spread <= MAX_SPREAD:
                    opp = Opportunity(
                        signal_type=SignalType.SPREAD_CAPTURE,
                        condition_id=cond_id,
                        question=question,
                        category=category,
                        token_id=token_ids[0] if token_ids else "",
                        side="YES",
                        market_price=mid,
                        fair_value=mid,
                        edge=round(net_spread, 5),
                        edge_pct=round(net_spread / mid, 4) if mid > 0 else 0,
                        best_bid=best_bid,
                        best_ask=best_ask,
                        spread=spread,
                        liquidity_usd=liquidity,
                        volume_24h=market.get("volume24hr") or 0,
                        notes=f"bid={best_bid:.3f} ask={best_ask:.3f} spread={spread:.3f} net={net_spread:.4f}"
                    )
                    opportunities.append(opp)

                # ── NO Farming: YES sobrevaluado ──────────────────────────
                outcome_prices_raw = market.get("outcomePrices")
                if outcome_prices_raw:
                    outcome_prices = json.loads(outcome_prices_raw) if isinstance(outcome_prices_raw, str) else outcome_prices_raw
                    if len(outcome_prices) == 2:
                        yes_price = float(outcome_prices[0])
                        no_price  = float(outcome_prices[1])
                        fair_no   = 1.0 - yes_price

                        if yes_price >= HYPE_YES_THRESHOLD and no_price < fair_no - self.min_edge:
                            edge_no = fair_no - no_price - self.fee_rate
                            if edge_no >= self.min_edge:
                                opp = Opportunity(
                                    signal_type=SignalType.OVERPRICED_NO,
                                    condition_id=cond_id,
                                    question=question,
                                    category=category,
                                    token_id=token_ids[1] if len(token_ids) > 1 else "",
                                    side="NO",
                                    market_price=no_price,
                                    fair_value=fair_no,
                                    edge=round(edge_no, 5),
                                    edge_pct=round(edge_no / no_price, 4) if no_price > 0 else 0,
                                    best_bid=best_bid,
                                    best_ask=best_ask,
                                    spread=spread,
                                    liquidity_usd=liquidity,
                                    volume_24h=market.get("volume24hr") or 0,
                                    notes=f"YES={yes_price:.3f} => fair_NO={fair_no:.3f} vs market_NO={no_price:.3f}"
                                )
                                opportunities.append(opp)

            except Exception as e:
                logger.debug(f"Spread skip {market.get('conditionId','?')[:12]}: {e}")

        opportunities.sort(key=lambda o: o.edge, reverse=True)
        return opportunities

    def _get_category(self, market: dict) -> str:
        events = market.get("events")
        if events and isinstance(events, list) and events:
            return events[0].get("category", "") or events[0].get("title", "")
        return ""
