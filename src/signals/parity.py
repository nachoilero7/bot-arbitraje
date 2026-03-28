"""
Signal: Parity Arbitrage
YES_price + NO_price < 1.00 despues de fees => comprar ambos, garantizar $1.

Con datos de Gamma API:
  outcomePrices: ["0.45", "0.55"]  -> YES=0.45, NO=0.55 (son mid-prices)
  bestAsk del YES token es el precio real de compra.

Dado que Gamma no siempre da el ask por outcome individual, usamos
outcomePrices como aproximacion del ask (levemente conservadora).
"""
import json
from src.signals.base import BaseSignal, Opportunity, SignalType
from src.utils.logger import get_logger

logger = get_logger(__name__)


class ParitySignal(BaseSignal):

    @property
    def name(self) -> str:
        return "PARITY"

    def detect(self, markets: list[dict], prices: dict = None) -> list[Opportunity]:
        opportunities = []

        for market in markets:
            try:
                outcome_prices_raw = market.get("outcomePrices")
                outcomes_raw = market.get("outcomes")
                if not outcome_prices_raw or not outcomes_raw:
                    continue

                outcome_prices = json.loads(outcome_prices_raw) if isinstance(outcome_prices_raw, str) else outcome_prices_raw
                outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw

                if len(outcome_prices) != 2 or len(outcomes) != 2:
                    continue  # Solo mercados binarios

                prices_float = [float(p) for p in outcome_prices]
                total_cost = sum(prices_float)

                # gap bruto y neto (2 compras = 2 fees)
                gross_gap = 1.0 - total_cost
                net_gap   = gross_gap - (2 * self.fee_rate)

                if net_gap >= self.min_edge:
                    token_ids = json.loads(market.get("clobTokenIds", "[]")) if isinstance(market.get("clobTokenIds"), str) else []
                    opp = Opportunity(
                        signal_type=SignalType.PARITY,
                        condition_id=market.get("conditionId", ""),
                        question=market.get("question", ""),
                        category=self._get_category(market),
                        token_id=token_ids[0] if token_ids else "",
                        token_id_b=token_ids[1] if len(token_ids) > 1 else "",
                        side="YES+NO",
                        market_price=prices_float[0],   # YES price (individual, not total)
                        price_b=prices_float[1],        # NO price
                        fair_value=1.0,
                        edge=round(net_gap, 5),
                        edge_pct=round(net_gap / total_cost, 4) if total_cost > 0 else 0,
                        best_bid=market.get("bestBid") or 0,
                        best_ask=market.get("bestAsk") or 0,
                        spread=market.get("spread") or 0,
                        liquidity_usd=market.get("liquidityNum") or 0,
                        volume_24h=market.get("volume24hr") or 0,
                        notes=(
                            f"{outcomes[0]}={prices_float[0]:.3f} + {outcomes[1]}={prices_float[1]:.3f}"
                            f" = {total_cost:.4f} | net_gap={net_gap:.4f}"
                            f" | liq=${market.get('liquidityNum',0):.0f}"
                        )
                    )
                    opportunities.append(opp)
                    logger.debug(f"[PARITY] {opp.question[:55]} | gap={net_gap:.4f}")

            except Exception as e:
                logger.debug(f"Parity skip {market.get('conditionId','?')[:12]}: {e}")

        opportunities.sort(key=lambda o: o.edge, reverse=True)
        return opportunities

    def _get_category(self, market: dict) -> str:
        events = market.get("events")
        if events and isinstance(events, list) and events:
            return events[0].get("category", "") or events[0].get("title", "")
        return ""
