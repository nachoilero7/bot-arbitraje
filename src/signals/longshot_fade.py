"""
Signal: Longshot Fade
Detecta YES tokens sistematicamente sobrevaluados por "longshot bias":
la gente paga de mas por eventos improbables (esperanza/recency bias).

Tambien detecta el inverso: mercados con YES muy bajo donde la gente
subvalua eventos poco probables.

Casos:
  HIGH (NO oportunidad): YES > 0.82, mercado de bajo volumen/liquidez
    → la gente sobrevalua eventos casi seguros en mercados poco eficientes
    → ajustar prob real con descuento del 12%
    → comprar NO

  LOW (YES oportunidad): YES < 0.12, mismas condiciones
    → la gente subvalua eventos muy improbables
    → ajustar prob real con prima del 15%
    → comprar YES
"""
import json
from src.signals.base import BaseSignal, Opportunity, SignalType
from src.utils.logger import get_logger

logger = get_logger(__name__)

LONGSHOT_HIGH_THRESHOLD = 0.82   # YES sobrevaluado si precio > 82%
LONGSHOT_LOW_THRESHOLD  = 0.12   # YES subvaluado si precio < 12%
MAX_VOLUME_24H          = 2000   # USD — mercado blando si volumen bajo
MAX_LIQUIDITY           = 5000   # USD — mercado pequeño
MIN_SPREAD              = 0.03   # spread minimo para confirmar mercado no eficiente
HIGH_DISCOUNT           = 0.88   # descuento 12% para mercados high-YES
LOW_PREMIUM             = 1.15   # prima 15% para mercados low-YES


class LongshotFadeSignal(BaseSignal):
    """
    Detecta oportunidades derivadas del sesgo cognitivo "longshot bias"
    en mercados de bajo volumen y liquidez reducida.
    """

    @property
    def name(self) -> str:
        return "LONGSHOT_FADE"

    def detect(self, markets: list[dict], prices: dict = None) -> list[Opportunity]:
        opportunities = []

        for market in markets:
            try:
                liquidity  = market.get("liquidityNum") or 0
                volume_24h = market.get("volume24hr") or 0
                spread     = market.get("spread") or 0

                # Condiciones de mercado blando (aplica a ambos casos)
                # Mercado blando: bajo volumen O baja liquidez, con spread minimo
                # AND era demasiado restrictivo � OR captura mercados ineficientes reales
                soft_market = (
                    (volume_24h < MAX_VOLUME_24H or liquidity < MAX_LIQUIDITY)
                    and spread > MIN_SPREAD
                    and liquidity > 100  # descartar mercados sin liquidez real
                )
                if not soft_market:
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
                no_price  = float(outcome_prices[1])
                if yes_price <= 0 or no_price <= 0:
                    continue

                token_ids = (
                    json.loads(market.get("clobTokenIds", "[]"))
                    if isinstance(market.get("clobTokenIds"), str)
                    else []
                )
                category   = self._get_category(market)
                question   = market.get("question", "")
                cond_id    = market.get("conditionId", "")
                best_bid   = market.get("bestBid") or 0
                best_ask   = market.get("bestAsk") or 0

                # ── Caso HIGH: YES sobrevaluado, comprar NO ───────────────
                if yes_price > LONGSHOT_HIGH_THRESHOLD:
                    est_true_prob = yes_price * HIGH_DISCOUNT
                    true_no_prob  = 1.0 - est_true_prob
                    edge          = true_no_prob - no_price - self.fee_rate

                    if edge >= self.min_edge:
                        opp = Opportunity(
                            signal_type=SignalType.OVERPRICED_NO,
                            condition_id=cond_id,
                            question=question,
                            category=category,
                            token_id=token_ids[1] if len(token_ids) > 1 else "",
                            side="NO",
                            market_price=no_price,
                            fair_value=round(true_no_prob, 5),
                            edge=round(edge, 5),
                            edge_pct=round(edge / no_price, 4) if no_price > 0 else 0,
                            best_bid=best_bid,
                            best_ask=best_ask,
                            spread=spread,
                            liquidity_usd=liquidity,
                            volume_24h=volume_24h,
                            notes=(
                                f"longshot_high | YES={yes_price:.3f} "
                                f"est_true={est_true_prob:.3f} "
                                f"fair_NO={true_no_prob:.3f} market_NO={no_price:.3f} "
                                f"edge={edge:.4f} | "
                                f"vol24h=${volume_24h:.0f} liq=${liquidity:.0f}"
                            )
                        )
                        opportunities.append(opp)
                        logger.info(
                            f"[LONGSHOT_FADE] HIGH {question[:50]} | "
                            f"yes={yes_price:.3f} fair_no={true_no_prob:.3f} edge={edge:.4f}"
                        )

                # ── Caso LOW: YES subvaluado, comprar YES ─────────────────
                elif yes_price < LONGSHOT_LOW_THRESHOLD:
                    est_true_prob = yes_price * LOW_PREMIUM
                    edge          = est_true_prob - yes_price - self.fee_rate

                    if edge >= self.min_edge:
                        opp = Opportunity(
                            signal_type=SignalType.PARITY,
                            condition_id=cond_id,
                            question=question,
                            category=category,
                            token_id=token_ids[0] if token_ids else "",
                            side="YES",
                            market_price=yes_price,
                            fair_value=round(est_true_prob, 5),
                            edge=round(edge, 5),
                            edge_pct=round(edge / yes_price, 4) if yes_price > 0 else 0,
                            best_bid=best_bid,
                            best_ask=best_ask,
                            spread=spread,
                            liquidity_usd=liquidity,
                            volume_24h=volume_24h,
                            notes=(
                                f"longshot_low | YES={yes_price:.3f} "
                                f"est_true={est_true_prob:.3f} "
                                f"edge={edge:.4f} | "
                                f"vol24h=${volume_24h:.0f} liq=${liquidity:.0f}"
                            )
                        )
                        opportunities.append(opp)
                        logger.info(
                            f"[LONGSHOT_FADE] LOW {question[:50]} | "
                            f"yes={yes_price:.3f} est_true={est_true_prob:.3f} edge={edge:.4f}"
                        )

            except Exception as e:
                logger.debug(f"LongshotFade skip {market.get('conditionId', '?')[:12]}: {e}")

        opportunities.sort(key=lambda o: o.edge, reverse=True)
        return opportunities

    def _get_category(self, market: dict) -> str:
        events = market.get("events")
        if events and isinstance(events, list) and events:
            return events[0].get("category", "") or events[0].get("title", "")
        return ""
