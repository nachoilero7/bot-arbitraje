"""
Signal: Price Drift / Momentum
Rastrea el historial de precios entre scans y detecta MOMENTUM:
cuando un precio se ha movido consistentemente en una direccion en
scans recientes, suele continuar (desequilibrio de order flow).

Cuando una caida rapida es detectada -> oportunidad de comprar YES (rebote).
Cuando una suba rapida es detectada  -> oportunidad de comprar NO (correccion).

Requiere minimo 4 puntos de historial por mercado para calcular deriva.
Solo opera en mercados con liquidez y volumen minimos.
"""
import json
import time
from src.signals.base import BaseSignal, Opportunity, SignalType
from src.utils.logger import get_logger

logger = get_logger(__name__)

MIN_HISTORY_POINTS     = 4      # Minimo de puntos para calcular drift
MIN_DRIFT_MAGNITUDE    = 0.05   # Cambio minimo en YES (subido de 0.04 para reducir ruido)
RECOVERY_FACTOR        = 0.5    # Recuperacion esperada como fraccion del drift
MAX_HISTORY_PER_MARKET = 20     # Maximo de entradas guardadas por mercado
MIN_LIQUIDITY          = 2000   # USD minimo de liquidez (subido de 500)
MIN_VOLUME_24H         = 1000   # USD minimo de volumen diario (subido de 100)
MIN_HISTORY_SPAN_SECS  = 300    # Los 4 puntos deben cubrir al menos 5 minutos
PRICE_FLOOR            = 0.07   # Ignorar mercados con YES < 7% (probablemente resolviendo)
PRICE_CEIL             = 0.93   # Ignorar mercados con YES > 93% (probablemente resolviendo)


class PriceDriftSignal(BaseSignal):
    """
    Detecta oportunidades de momentum/mean-reversion basadas en movimientos
    de precio consistentes a lo largo de multiples scans consecutivos.
    """

    def __init__(self, fee_rate: float = 0.02, min_edge: float = 0.03):
        super().__init__(fee_rate=fee_rate, min_edge=min_edge)
        self._history: dict[str, list[tuple[float, float]]] = {}

    @property
    def name(self) -> str:
        return "PRICE_DRIFT"

    def detect(self, markets: list[dict], prices: dict = None) -> list[Opportunity]:
        opportunities = []
        now = time.time()

        for market in markets:
            try:
                cond_id = market.get("conditionId", "")
                if not cond_id:
                    continue

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
                if yes_price <= 0:
                    continue

                # Ignorar mercados cerca de resolución — su movimiento de precio
                # refleja el resultado, no un desequilibrio de order flow
                if yes_price < PRICE_FLOOR or yes_price > PRICE_CEIL:
                    continue

                # ── Actualizar historial ──────────────────────────────────
                if cond_id not in self._history:
                    self._history[cond_id] = []

                self._history[cond_id].append((now, yes_price))

                # Mantener maximo de entradas por mercado
                if len(self._history[cond_id]) > MAX_HISTORY_PER_MARKET:
                    self._history[cond_id] = self._history[cond_id][-MAX_HISTORY_PER_MARKET:]

                history = self._history[cond_id]
                if len(history) < MIN_HISTORY_POINTS:
                    continue

                # Los puntos deben cubrir al menos 5 minutos — evita actuar sobre
                # ruido de 20 segundos (4 scans * 5s = solo ruido de spread)
                oldest_ts, newest_ts = history[-MIN_HISTORY_POINTS][0], history[-1][0]
                if newest_ts - oldest_ts < MIN_HISTORY_SPAN_SECS:
                    continue

                # ── Filtros de liquidez ───────────────────────────────────
                liquidity  = market.get("liquidityNum") or 0
                volume_24h = market.get("volume24hr") or 0
                if liquidity < MIN_LIQUIDITY or volume_24h < MIN_VOLUME_24H:
                    continue

                # ── Analizar ultimos N puntos ─────────────────────────────
                recent = [p for _, p in history[-MIN_HISTORY_POINTS:]]
                drift_magnitude = abs(recent[-1] - recent[0])

                if drift_magnitude < MIN_DRIFT_MAGNITUDE:
                    continue

                token_ids = (
                    json.loads(market.get("clobTokenIds", "[]"))
                    if isinstance(market.get("clobTokenIds"), str)
                    else []
                )
                category = self._get_category(market)
                question = market.get("question", "")
                best_bid = market.get("bestBid") or 0
                best_ask = market.get("bestAsk") or 0
                spread   = market.get("spread") or 0

                # ── Downtrend: cada precio <= anterior (caida consistente) ─
                is_downtrend = all(
                    recent[i] <= recent[i - 1] for i in range(1, len(recent))
                )
                if is_downtrend:
                    # Precio cayo rapido → comprar YES esperando rebote parcial
                    fair_value = yes_price + drift_magnitude * RECOVERY_FACTOR
                    edge       = fair_value - yes_price - self.fee_rate

                    if edge >= self.min_edge:
                        opp = Opportunity(
                            signal_type=SignalType.PRICE_DRIFT,
                            condition_id=cond_id,
                            question=question,
                            category=category,
                            token_id=token_ids[0] if token_ids else "",
                            side="YES",
                            market_price=yes_price,
                            fair_value=round(fair_value, 5),
                            edge=round(edge, 5),
                            edge_pct=round(edge / yes_price, 4) if yes_price > 0 else 0,
                            best_bid=best_bid,
                            best_ask=best_ask,
                            spread=spread,
                            liquidity_usd=liquidity,
                            volume_24h=volume_24h,
                            notes=(
                                f"drift_down | yes={yes_price:.3f} "
                                f"drop={drift_magnitude:.4f} over {MIN_HISTORY_POINTS} scans "
                                f"fair={fair_value:.3f} edge={edge:.4f} | "
                                f"history={[round(p, 3) for p in recent]}"
                            )
                        )
                        opportunities.append(opp)
                        logger.info(
                            f"[PRICE_DRIFT] DOWN {question[:50]} | "
                            f"yes={yes_price:.3f} drop={drift_magnitude:.4f} edge={edge:.4f}"
                        )
                    continue

                # ── Uptrend: cada precio >= anterior (subida consistente) ──
                is_uptrend = all(
                    recent[i] >= recent[i - 1] for i in range(1, len(recent))
                )
                if is_uptrend:
                    # Precio subio rapido → comprar NO esperando correccion parcial
                    no_price  = 1.0 - yes_price
                    fair_no   = no_price + drift_magnitude * RECOVERY_FACTOR
                    edge      = fair_no - no_price - self.fee_rate

                    if edge >= self.min_edge:
                        opp = Opportunity(
                            signal_type=SignalType.PRICE_DRIFT,
                            condition_id=cond_id,
                            question=question,
                            category=category,
                            token_id=token_ids[1] if len(token_ids) > 1 else "",
                            side="NO",
                            market_price=no_price,
                            fair_value=round(fair_no, 5),
                            edge=round(edge, 5),
                            edge_pct=round(edge / no_price, 4) if no_price > 0 else 0,
                            best_bid=best_bid,
                            best_ask=best_ask,
                            spread=spread,
                            liquidity_usd=liquidity,
                            volume_24h=volume_24h,
                            notes=(
                                f"drift_up | yes={yes_price:.3f} "
                                f"rise={drift_magnitude:.4f} over {MIN_HISTORY_POINTS} scans "
                                f"no={no_price:.3f} fair_no={fair_no:.3f} edge={edge:.4f} | "
                                f"history={[round(p, 3) for p in recent]}"
                            )
                        )
                        opportunities.append(opp)
                        logger.info(
                            f"[PRICE_DRIFT] UP {question[:50]} | "
                            f"yes={yes_price:.3f} rise={drift_magnitude:.4f} edge={edge:.4f}"
                        )

            except Exception as e:
                logger.debug(f"PriceDrift skip {market.get('conditionId', '?')[:12]}: {e}")

        opportunities.sort(key=lambda o: o.edge, reverse=True)
        return opportunities

    def _get_category(self, market: dict) -> str:
        events = market.get("events")
        if events and isinstance(events, list) and events:
            return events[0].get("category", "") or events[0].get("title", "")
        return ""
