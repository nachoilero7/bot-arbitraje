"""
Signal: Cross-Platform Forecast Divergence
Detecta cuando los precios de Polymarket divergen significativamente de otras
plataformas de prediccion (Manifold Markets, Metaculus, etc).

Referencia: Dillon et al. (2023) — cuando Metaculus y Polymarket divergen >8pp,
la fuente externa tiene razon el 63% de las veces. El mismo efecto se observa
en Manifold Markets (cross-platform price discovery).

Logica:
  1. Para cada mercado activo con suficiente liquidez y volumen
  2. Obtener probabilidad del enricher externo via busqueda semantica
  3. Si divergencia > 8pp => hay edge potencial
  4. Generar Opportunity con side YES o NO segun direccion

Enricher compatible: cualquier objeto con metodo find_probability(question: str) -> float | None
  - ManifoldClient (por defecto, sin auth)
  - MetaculusClient (requiere token desde 2024)
"""
import json

from src.signals.base import BaseSignal, Opportunity, SignalType
from src.utils.logger import get_logger

logger = get_logger(__name__)

MIN_DIVERGENCE     = 0.08   # 8pp threshold (Dillon et al. 2023)
MIN_VOLUME_24H     = 500
MIN_LIQUIDITY      = 1000
PRICE_FLOOR        = 0.05   # Skip extreme prices — cobertura externa pobre
PRICE_CEIL         = 0.95
MAX_QUERIES_PER_SCAN = 40   # Cap de consultas por scan — evita bloquear el loop


class MetaculusDivergenceSignal(BaseSignal):
    """Reutiliza el nombre historico; acepta cualquier enricher con find_probability()."""

    def __init__(self, enricher, source_name: str = "MANIFOLD", fee_rate: float = 0.02, min_edge: float = 0.03):
        super().__init__(fee_rate=fee_rate, min_edge=min_edge)
        self.enricher    = enricher
        self.source_name = source_name

    @property
    def name(self) -> str:
        return "METACULUS_DIV"

    def detect(self, markets: list[dict], prices: dict = None) -> list[Opportunity]:
        opportunities = []

        # Priorizar los mercados con mayor volumen; cap de API calls por scan
        # (mercados ya cacheados no cuentan contra el cap)
        sorted_markets = sorted(
            markets,
            key=lambda m: float(m.get("volume24hr") or 0),
            reverse=True,
        )
        api_calls_remaining = MAX_QUERIES_PER_SCAN

        for market in sorted_markets:
            try:
                # Liquidity and volume filters
                volume_24h  = float(market.get("volume24hr") or 0)
                liquidity   = float(market.get("liquidityNum") or 0)
                if volume_24h < MIN_VOLUME_24H or liquidity < MIN_LIQUIDITY:
                    continue

                question = market.get("question", "")
                if not question:
                    continue

                # Parse YES price from outcomePrices
                outcome_prices_raw = market.get("outcomePrices")
                if not outcome_prices_raw:
                    continue
                outcome_prices = (
                    json.loads(outcome_prices_raw)
                    if isinstance(outcome_prices_raw, str)
                    else outcome_prices_raw
                )
                if not outcome_prices:
                    continue
                yes_price = float(outcome_prices[0])

                # Skip extreme prices where Metaculus coverage is poor
                if yes_price <= PRICE_FLOOR or yes_price >= PRICE_CEIL:
                    continue

                # Query enricher externo (Manifold, Metaculus, etc)
                # Si el enricher tiene cache interno, la consulta es instantanea.
                # Si no esta en cache, consume un slot del cap por scan.
                cache_key = question.strip().lower()
                in_cache  = cache_key in getattr(self.enricher, "_cache", {})
                if not in_cache:
                    if api_calls_remaining <= 0:
                        continue  # cap alcanzado, dejar para el proximo scan
                    api_calls_remaining -= 1

                ext_prob = self.enricher.find_probability(question)
                if ext_prob is None:
                    continue

                divergence = ext_prob - yes_price

                # Determine direction and edge
                if divergence > MIN_DIVERGENCE:
                    side        = "YES"
                    market_price = yes_price
                    fair_value   = ext_prob
                    edge         = divergence - self.fee_rate
                elif divergence < -MIN_DIVERGENCE:
                    side        = "NO"
                    market_price = 1.0 - yes_price
                    fair_value   = 1.0 - ext_prob
                    edge         = abs(divergence) - self.fee_rate
                else:
                    continue

                if edge < self.min_edge:
                    continue

                # Parse token IDs
                clob_raw = market.get("clobTokenIds")
                token_ids = (
                    json.loads(clob_raw)
                    if isinstance(clob_raw, str)
                    else (clob_raw or [])
                )
                token_id = token_ids[0] if side == "YES" else (token_ids[1] if len(token_ids) > 1 else "")

                category = ""
                events = market.get("events")
                if events and isinstance(events, list) and events:
                    category = events[0].get("category", "") or ""

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
                    edge_pct=round(edge / market_price, 4) if market_price > 0 else 0.0,
                    liquidity_usd=liquidity,
                    volume_24h=volume_24h,
                    notes=(
                        f"{self.source_name} DIVERGENCE | "
                        f"poly={yes_price:.3f} ext={ext_prob:.3f} div={divergence:+.3f}"
                    ),
                )
                opportunities.append(opp)
                logger.info(
                    f"[{self.source_name}_DIV] {question[:55]} | "
                    f"poly={yes_price:.3f} ext={ext_prob:.3f} "
                    f"div={divergence:+.3f} side={side} edge={edge:.4f}"
                )

            except Exception as e:
                logger.debug(f"{self.source_name}Divergence skip market: {e}")

        opportunities.sort(key=lambda o: o.edge, reverse=True)
        logger.info(f"[{self.source_name}_DIV] {len(opportunities)} opportunities found")
        return opportunities
