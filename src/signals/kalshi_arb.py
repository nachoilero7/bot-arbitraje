"""
Signal: Kalshi Cross-Market Arbitrage
Compara precios del mismo evento en Polymarket y Kalshi.
Si poly_price < kalshi_yes - fee => comprar YES en Polymarket.
Si kalshi_yes < poly_price - fee => comprar en Kalshi (Phase 2).

Kalshi es un mercado regulado por CFTC con precios independientes
que a veces divergen de Polymarket por 5-15 puntos.
"""
import json
from dataclasses import dataclass

from src.signals.base import BaseSignal, Opportunity, SignalType
from src.enrichers.kalshi import KalshiApiClient
from src.utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class KalshiOpportunity(Opportunity):
    """Opportunity enriquecida con datos de Kalshi para arbitraje cruzado."""
    kalshi_url: str = ""
    kalshi_price: float = 0.0


class KalshiArbSignal(BaseSignal):
    """
    Detecta divergencias de precio entre Polymarket y Kalshi para el
    mismo evento subyacente. Comprar en el mercado más barato.

    Fase 1: solo comprar YES en Polymarket cuando es más barato que Kalshi.
    Fase 2 (futuro): también operar en Kalshi cuando Poly es más caro.
    """

    def __init__(self, kalshi_client: KalshiApiClient, fee_rate: float = 0.02, min_edge: float = 0.03):
        super().__init__(fee_rate=fee_rate, min_edge=min_edge)
        self.kalshi_client = kalshi_client

    @property
    def name(self) -> str:
        return "KALSHI_ARB"

    def detect(self, markets: list[dict], prices: dict = None) -> list[Opportunity]:
        opportunities = []

        # Fetch todos los mercados Kalshi activos una sola vez por scan (cacheado en cliente)
        try:
            kalshi_markets = self.kalshi_client.get_active_markets()
        except Exception as e:
            logger.warning(f"KalshiArb: no se pudieron obtener mercados Kalshi: {e}")
            return []

        if not kalshi_markets:
            return []

        for market in markets:
            try:
                question = market.get("question", "")
                if not question:
                    continue

                # Buscar mercado equivalente en Kalshi
                kalshi_match = self.kalshi_client.find_matching_market(question, kalshi_markets)
                if not kalshi_match:
                    continue

                kalshi_yes = kalshi_match.get("yes_price", 0.0)
                if kalshi_yes <= 0:
                    continue

                # Precio YES en Polymarket
                outcome_prices_raw = market.get("outcomePrices")
                if not outcome_prices_raw:
                    continue
                outcome_prices = json.loads(outcome_prices_raw) if isinstance(outcome_prices_raw, str) else outcome_prices_raw
                if len(outcome_prices) < 2:
                    continue

                poly_yes_price = float(outcome_prices[0])
                if poly_yes_price <= 0 or poly_yes_price >= 0.98:
                    continue

                # Fase 1: comprar YES en Polymarket si es más barato que Kalshi
                edge = kalshi_yes - poly_yes_price - self.fee_rate
                if edge < self.min_edge:
                    continue

                token_ids = json.loads(market.get("clobTokenIds", "[]")) if isinstance(market.get("clobTokenIds"), str) else []
                category = self._get_category(market)
                kalshi_url = kalshi_match.get("url", "")

                opp = KalshiOpportunity(
                    signal_type=SignalType.MISPRICED_CORR,
                    condition_id=market.get("conditionId", ""),
                    question=question,
                    category=category,
                    token_id=token_ids[0] if token_ids else "",
                    side="YES",
                    market_price=poly_yes_price,
                    fair_value=kalshi_yes,
                    edge=round(edge, 5),
                    edge_pct=round(edge / poly_yes_price, 4) if poly_yes_price > 0 else 0,
                    best_bid=market.get("bestBid") or 0,
                    best_ask=market.get("bestAsk") or 0,
                    spread=market.get("spread") or 0,
                    liquidity_usd=market.get("liquidityNum") or 0,
                    volume_24h=market.get("volume24hr") or 0,
                    notes=(
                        f"Poly={poly_yes_price:.3f} | Kalshi={kalshi_yes:.3f} | "
                        f"edge={edge:.4f} | {kalshi_url}"
                    ),
                    kalshi_url=kalshi_url,
                    kalshi_price=kalshi_yes,
                )
                opportunities.append(opp)
                logger.info(
                    f"[KALSHI_ARB] {question[:50]} | "
                    f"poly={poly_yes_price:.3f} kalshi={kalshi_yes:.3f} edge={edge:.4f}"
                )

            except Exception as e:
                logger.debug(f"KalshiArb skip {market.get('conditionId','?')[:12]}: {e}")

        opportunities.sort(key=lambda o: o.edge, reverse=True)
        return opportunities

    def _get_category(self, market: dict) -> str:
        events = market.get("events")
        if events and isinstance(events, list) and events:
            return events[0].get("category", "") or events[0].get("title", "")
        return ""
