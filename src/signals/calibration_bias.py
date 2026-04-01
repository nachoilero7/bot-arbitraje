"""
Signal: Calibration Bias (basado en SSRN 5910522 — 124 millones de trades en Polymarket)

Hallazgo empirico: los precios de Polymarket tienen sesgos sistematicos de calibracion:

  - Eventos con YES < 10%: el mercado los SUBESTIMA.
    Ocurren el 14% de las veces cuando el mercado dice 10% → subvaluados ~40%.
    → COMPRAR YES en mercados de baja probabilidad con liquidez real.

  - Eventos con YES > 80%: el mercado los SOBREVALUA ligeramente (~3-5%).
    → COMPRAR NO en mercados de alta probabilidad con suficiente spread.

  - Zona 10%-80%: bien calibrada, sin edge sistematico.

Diferencia con LongshotFade:
  - LongshotFade requiere condiciones de mercado "blando" (low vol/liq/spread).
  - CalibrationBias aplica a TODOS los mercados con liquidez suficiente,
    ya que el sesgo existe independientemente del tamano del mercado.
  - Los multiplicadores de ajuste estan basados en datos empiricos (no arbitrarios).

Fuente: Reichenbach & Walther, "Exploring Decentralized Prediction Markets:
Accuracy, Skill, and Bias on Polymarket", SSRN 5910522 (2024/2025), 124M trades.
"""
import json

from src.signals.base import BaseSignal, Opportunity, SignalType
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Umbrales basados en SSRN 5910522
LOW_PROB_THRESHOLD  = 0.10   # Debajo de esto: mercado subestima el evento
HIGH_PROB_THRESHOLD = 0.80   # Arriba de esto: mercado sobrevalua el evento

# Ajustes empiricos: eventos <10% ocurren 14% del tiempo → factor 1.40
LOW_PROB_ADJUSTMENT  = 1.40  # P_real ≈ P_market × 1.40 para eventos raros
HIGH_PROB_ADJUSTMENT = 0.97  # P_real ≈ P_market × 0.97 para favoritos

# Filtros minimos — el sesgo existe pero fees lo erosionan en mercados iliquidos
MIN_LIQUIDITY = 1_000   # USD — necesitamos poder ejecutar
MIN_VOLUME_24H = 200    # USD — mercado activo
MAX_SPREAD = 0.20       # no entrar en mercados con spread demasiado amplio

# Per-category calibration adjustments based on empirical research:
# Leigh & Wolfers (2006) - political markets; Barber et al. (2022) - retail attention
# Values: (yes_adjustment, no_adjustment) — positive = market underprices, buy that side
_CATEGORY_ADJUSTMENTS: dict[str, tuple[float, float]] = {
    # Politics: incumbents & frontrunners overvalued; underdogs undervalued
    "politics":     (-0.03, +0.03),   # fade YES on favorites
    "election":     (-0.03, +0.03),
    "government":   (-0.02, +0.02),
    # Sports: home team & favorite overvalued in high-profile events
    "sports":       (-0.02, +0.02),
    "soccer":       (-0.02, +0.02),
    "basketball":   (-0.02, +0.02),
    "football":     (-0.02, +0.02),
    # Crypto: extreme outcomes have higher calibration error (tail risk underestimated)
    "crypto":       (+0.01, -0.01),   # slight YES boost (black swans more likely)
    "bitcoin":      (+0.01, -0.01),
    # Economics: near-term well calibrated, no strong adjustment
    "economics":    (0.0, 0.0),
    # Default (no category match): no adjustment
}


class CalibrationBiasSignal(BaseSignal):
    """
    Explota el sesgo sistematico de calibracion documentado en 124M trades de Polymarket.

    Estrategia conservadora:
      - Solo opera donde la liquidez permite ejecucion limpia
      - Edge minimo = fee_rate para garantizar profitabilidad esperada
      - No opera en zona bien calibrada (10%-80%)
    """

    @property
    def name(self) -> str:
        return "CALIBRATION_BIAS"

    def detect(self, markets: list[dict], prices: dict = None) -> list[Opportunity]:
        opportunities = []

        for market in markets:
            try:
                liquidity  = float(market.get("liquidityNum") or 0)
                volume_24h = float(market.get("volume24hr") or 0)
                spread     = float(market.get("spread") or 1.0)

                if liquidity < MIN_LIQUIDITY or volume_24h < MIN_VOLUME_24H:
                    continue
                if spread > MAX_SPREAD:
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
                    else (market.get("clobTokenIds") or [])
                )
                cond_id  = market.get("conditionId", "")
                question = market.get("question", "")
                category = self._get_category(market)
                best_bid = float(market.get("bestBid") or 0)
                best_ask = float(market.get("bestAsk") or 0)

                yes_adj, no_adj = self._get_category_adjustment(market)

                # ── Caso LOW: YES < 10% → mercado subestima, comprar YES ──
                if yes_price < LOW_PROB_THRESHOLD:
                    true_yes      = min(yes_price * LOW_PROB_ADJUSTMENT, LOW_PROB_THRESHOLD * 1.5)
                    edge          = true_yes - yes_price - self.fee_rate
                    adjusted_edge = edge + yes_adj

                    if adjusted_edge >= self.min_edge:
                        edge_pct = adjusted_edge / yes_price if yes_price > 0 else 0
                        opp = Opportunity(
                            signal_type=SignalType.CALIBRATION_BIAS,
                            condition_id=cond_id,
                            question=question,
                            category=category,
                            token_id=token_ids[0] if token_ids else "",
                            side="YES",
                            market_price=yes_price,
                            fair_value=round(true_yes, 5),
                            edge=round(adjusted_edge, 5),
                            edge_pct=round(edge_pct, 4),
                            best_bid=best_bid,
                            best_ask=best_ask,
                            spread=spread,
                            liquidity_usd=liquidity,
                            volume_24h=volume_24h,
                            notes=(
                                f"calib_low | YES={yes_price:.3f} "
                                f"true_est={true_yes:.3f} "
                                f"adj={LOW_PROB_ADJUSTMENT:.2f}x | "
                                f"edge={adjusted_edge:.4f} | SSRN5910522"
                                f" | cat_adj={yes_adj:+.2f}"
                            ),
                        )
                        opportunities.append(opp)
                        logger.info(
                            f"[CALIBRATION_BIAS] LOW {question[:50]} | "
                            f"yes={yes_price:.3f} est={true_yes:.3f} edge={adjusted_edge:.4f}"
                        )

                # ── Caso HIGH: YES > 80% → mercado sobrevalua, comprar NO ─
                elif yes_price > HIGH_PROB_THRESHOLD:
                    true_yes      = yes_price * HIGH_PROB_ADJUSTMENT
                    true_no       = 1.0 - true_yes
                    edge          = true_no - no_price - self.fee_rate
                    adjusted_edge = edge + no_adj

                    if adjusted_edge >= self.min_edge:
                        edge_pct = adjusted_edge / no_price if no_price > 0 else 0
                        no_token = token_ids[1] if len(token_ids) > 1 else ""
                        opp = Opportunity(
                            signal_type=SignalType.CALIBRATION_BIAS,
                            condition_id=cond_id,
                            question=question,
                            category=category,
                            token_id=no_token,
                            side="NO",
                            market_price=no_price,
                            fair_value=round(true_no, 5),
                            edge=round(adjusted_edge, 5),
                            edge_pct=round(edge_pct, 4),
                            best_bid=best_bid,
                            best_ask=best_ask,
                            spread=spread,
                            liquidity_usd=liquidity,
                            volume_24h=volume_24h,
                            notes=(
                                f"calib_high | YES={yes_price:.3f} "
                                f"true_yes={true_yes:.3f} fair_NO={true_no:.3f} "
                                f"market_NO={no_price:.3f} | "
                                f"edge={adjusted_edge:.4f} | SSRN5910522"
                                f" | cat_adj={no_adj:+.2f}"
                            ),
                        )
                        opportunities.append(opp)
                        logger.info(
                            f"[CALIBRATION_BIAS] HIGH {question[:50]} | "
                            f"yes={yes_price:.3f} true_no={true_no:.3f} edge={adjusted_edge:.4f}"
                        )

            except Exception as e:
                logger.debug(f"CalibrationBias skip {market.get('conditionId', '?')[:12]}: {e}")

        opportunities.sort(key=lambda o: o.edge, reverse=True)
        return opportunities

    def _get_category(self, market: dict) -> str:
        events = market.get("events")
        if events and isinstance(events, list) and events:
            return events[0].get("category", "") or events[0].get("title", "")
        return ""

    def _get_category_adjustment(self, market: dict) -> tuple[float, float]:
        """
        Return (yes_adjustment, no_adjustment) for the market's category.

        Checks category text from multiple fields, lowercased substring match
        against _CATEGORY_ADJUSTMENTS keys.
        """
        candidates: list[str] = []

        events = market.get("events")
        if events and isinstance(events, list) and events:
            event = events[0]
            if event.get("category"):
                candidates.append(str(event["category"]).lower())
            if event.get("title"):
                candidates.append(str(event["title"]).lower())

        if market.get("category"):
            candidates.append(str(market["category"]).lower())
        if market.get("question"):
            candidates.append(str(market["question"]).lower())

        for text in candidates:
            for key, adjustment in _CATEGORY_ADJUSTMENTS.items():
                if key in text:
                    return adjustment

        return (0.0, 0.0)
