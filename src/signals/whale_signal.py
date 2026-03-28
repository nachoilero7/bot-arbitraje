"""
Signal: Whale Signal
Detecta cuando grandes operadores (ballenas) estan posicionados fuertemente
en un lado de un mercado, sugiriendo un mispricing que el mercado aun no refleja.

Logica:
  - ratio >= 0.72 → ballenas comprando YES masivamente → señal YES
  - ratio <= 0.28 → ballenas vendiendo YES masivamente → señal NO (comprar NO)
  - Se requieren al menos 3 trades whale recientes para actuar

Tipo de señal: MISPRICED_CORR
"""
import json

from src.enrichers.whale_tracker import WhaleTracker
from src.signals.base import BaseSignal, Opportunity, SignalType
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Umbrales de presion
RATIO_BUY_THRESHOLD  = 0.72  # >= este valor → señal YES
RATIO_SELL_THRESHOLD = 0.28  # <= este valor → señal NO
MIN_WHALE_TRADES     = 3     # trades whale minimos para confiar en la señal

# Ajuste maximo de fair value basado en presion
PRESSURE_SCALE = 0.20        # (ratio - 0.5) * PRESSURE_SCALE = ajuste en probabilidad
MAX_FAIR_VALUE = 0.97        # Nunca estimar probabilidad mayor a 0.97

# Filtros de mercado
MIN_LIQUIDITY_USD = 1_000
MIN_VOLUME_24H    = 200


class WhaleSignal(BaseSignal):
    """
    Detecta oportunidades cuando las ballenas se posicionan fuertemente
    en un lado y el precio de mercado aun no refleja esa informacion.
    """

    def __init__(
        self,
        whale_tracker: WhaleTracker,
        fee_rate:     float = 0.02,
        min_edge:     float = 0.03,
        max_per_scan: int   = 15,
    ):
        super().__init__(fee_rate=fee_rate, min_edge=min_edge)
        self.whale_tracker = whale_tracker
        self.max_per_scan  = max_per_scan

    @property
    def name(self) -> str:
        return "WHALE_SIGNAL"

    def detect(self, markets: list[dict], prices: dict = None) -> list[Opportunity]:
        opportunities = []

        # Filtrar mercados con liquidez y volumen minimo, luego ordenar por liquidez desc
        eligible = [
            m for m in markets
            if (m.get("liquidityNum") or 0) >= MIN_LIQUIDITY_USD
            and (m.get("volume24hr") or 0) >= MIN_VOLUME_24H
        ]
        eligible.sort(key=lambda m: m.get("liquidityNum") or 0, reverse=True)
        candidates = eligible[: self.max_per_scan]

        for market in candidates:
            try:
                # Extraer yes_token_id (primer elemento de clobTokenIds)
                raw_token_ids = market.get("clobTokenIds", "[]")
                token_ids = (
                    json.loads(raw_token_ids)
                    if isinstance(raw_token_ids, str)
                    else raw_token_ids
                )
                if not token_ids:
                    continue
                yes_token_id = token_ids[0]

                # Obtener presion whale
                pressure = self.whale_tracker.get_whale_pressure(yes_token_id)
                if pressure["whale_count"] < MIN_WHALE_TRADES:
                    continue

                # Extraer precio YES actual
                outcome_prices_raw = market.get("outcomePrices")
                if not outcome_prices_raw:
                    continue
                outcome_prices = (
                    json.loads(outcome_prices_raw)
                    if isinstance(outcome_prices_raw, str)
                    else outcome_prices_raw
                )
                if not outcome_prices or len(outcome_prices) < 1:
                    continue
                yes_price = float(outcome_prices[0])
                if yes_price <= 0 or yes_price >= 0.98:
                    continue

                ratio    = pressure["ratio"]
                category = self._get_category(market)
                question = market.get("question", "")

                opp = None

                # ── Señal YES: ballenas comprando ──────────────────────────────
                if ratio >= RATIO_BUY_THRESHOLD:
                    adjustment = (ratio - 0.5) * PRESSURE_SCALE
                    fair_value = min(MAX_FAIR_VALUE, yes_price + adjustment)
                    edge       = fair_value - yes_price - self.fee_rate

                    if edge >= self.min_edge:
                        opp = Opportunity(
                            signal_type=SignalType.MISPRICED_CORR,
                            condition_id=market.get("conditionId", ""),
                            question=question,
                            category=category,
                            token_id=yes_token_id,
                            side="YES",
                            market_price=yes_price,
                            fair_value=round(fair_value, 5),
                            edge=round(edge, 5),
                            edge_pct=round(edge / yes_price, 4) if yes_price > 0 else 0,
                            best_bid=market.get("bestBid") or 0,
                            best_ask=market.get("bestAsk") or 0,
                            spread=market.get("spread") or 0,
                            liquidity_usd=market.get("liquidityNum") or 0,
                            volume_24h=market.get("volume24hr") or 0,
                            notes=(
                                f"Whale BUY pressure | ratio={ratio:.3f} | "
                                f"whale_count={pressure['whale_count']} | "
                                f"buy_vol={pressure['buy_volume']:.0f} USDC | "
                                f"poly={yes_price:.3f} fair={fair_value:.3f} edge={edge:.4f}"
                            ),
                        )

                # ── Señal NO: ballenas vendiendo ───────────────────────────────
                elif ratio <= RATIO_SELL_THRESHOLD:
                    no_price   = 1.0 - yes_price
                    adjustment = (0.5 - ratio) * PRESSURE_SCALE
                    fair_no    = min(MAX_FAIR_VALUE, no_price + adjustment)
                    edge       = fair_no - no_price - self.fee_rate

                    if edge >= self.min_edge and no_price > 0:
                        no_token_id = token_ids[1] if len(token_ids) > 1 else yes_token_id
                        opp = Opportunity(
                            signal_type=SignalType.MISPRICED_CORR,
                            condition_id=market.get("conditionId", ""),
                            question=question,
                            category=category,
                            token_id=no_token_id,
                            side="NO",
                            market_price=no_price,
                            fair_value=round(fair_no, 5),
                            edge=round(edge, 5),
                            edge_pct=round(edge / no_price, 4) if no_price > 0 else 0,
                            best_bid=market.get("bestBid") or 0,
                            best_ask=market.get("bestAsk") or 0,
                            spread=market.get("spread") or 0,
                            liquidity_usd=market.get("liquidityNum") or 0,
                            volume_24h=market.get("volume24hr") or 0,
                            notes=(
                                f"Whale SELL pressure | ratio={ratio:.3f} | "
                                f"whale_count={pressure['whale_count']} | "
                                f"sell_vol={pressure['sell_volume']:.0f} USDC | "
                                f"no_price={no_price:.3f} fair_no={fair_no:.3f} edge={edge:.4f}"
                            ),
                        )

                if opp is not None:
                    opportunities.append(opp)
                    logger.info(
                        f"[WHALE_SIGNAL] {question[:50]} | side={opp.side} "
                        f"ratio={ratio:.3f} whales={pressure['whale_count']} "
                        f"edge={opp.edge:.4f}"
                    )

            except Exception as e:
                logger.debug(
                    f"WhaleSignal skip {market.get('conditionId', '?')[:12]}: {e}"
                )

        opportunities.sort(key=lambda o: o.edge, reverse=True)
        return opportunities

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _get_category(self, market: dict) -> str:
        events = market.get("events")
        if events and isinstance(events, list) and events:
            return events[0].get("category", "") or events[0].get("title", "")
        return ""
