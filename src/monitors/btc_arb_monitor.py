"""
BTC Arb Monitor v4 — Formula correcta para opciones binarias digitales.

Modelo matematico:
  El mercado "Will BTC be above $K on <date>?" es una opcion digital cash-or-nothing.
  La probabilidad correcta es N(d₂) de Black-Scholes, con correccion de Cornish-Fisher
  para los fat tails de BTC (kurtosis ~6-10 vs normal ~3).

  d₂ = [ln(S/K) - (σ²/2)·T] / (σ·√T)
  z_CF = d₂ - (g₁/6)·(d₂²-1) - (g₂/24)·(d₂³-3d₂) + (g₁²/36)·(2d₂³-5d₂)
  P(BTC > K) = N(z_CF)

  donde g₁=skewness(-0.2), g₂=kurtosis_excess(4.0) para BTC.

Volatilidad (HAR-RV — Corsi 2009):
  σ_HAR = β_d·RV_1d + β_w·RV_5d + β_m·RV_22d
  Calculada sobre velas de 5 minutos para capturar intraday dynamics.
  Mas precisa que vol diaria simple para prediccion de movimiento intradía.

Requisitos para operar:
  - Solo mercados DIARIOS con volumen >= $100K (mercados cortos tienen fees dinamicos 3.15%)
  - Edge neto >= MIN_EDGE (default: 9%)
  - 2 evaluaciones consecutivas confirman el signal
  - Blackout 10min antes del cierre
  - No operar en las primeras 2h del dia (alta incertidumbre)
  - Ajuste de funding rate solo en extremos (>0.05%/8h o <-0.03%/8h)
"""
import json
import math
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

from src.utils.logger import get_logger

logger = get_logger(__name__)

# ── Binance ─────────────────────────────────────────────────────────────────
BINANCE_REST_TICKER = "https://api.binance.com/api/v3/ticker/price"
BINANCE_KLINES      = "https://api.binance.com/api/v3/klines"
BINANCE_FUNDING     = "https://fapi.binance.com/fapi/v1/fundingRate"

# ── Polymarket ───────────────────────────────────────────────────────────────
GAMMA_SEARCH_URL = (
    "https://gamma-api.polymarket.com/markets"
    "?active=true&closed=false&limit=200&order=volume24hr&ascending=false"
)

# ── HAR-RV parametros (Corsi 2009) ───────────────────────────────────────────
HAR_BETA_DAILY   = 0.35   # componente diaria (velocidad de reaccion)
HAR_BETA_WEEKLY  = 0.25   # componente semanal (5 dias)
HAR_BETA_MONTHLY = 0.20   # componente mensual (22 dias)
HAR_INTERCEPT    = 0.005  # floor minimo de vol
BTC_DAILY_VOL_FALLBACK = 0.025  # 2.5%/dia si no se puede calcular

# ── Cornish-Fisher fat-tail correction ───────────────────────────────────────
# BTC tiene skewness negativo (crashes mas frecuentes que rallies) y fat tails
BTC_SKEWNESS        = -0.2    # g₁: skewness empirico de retornos BTC diarios
BTC_KURTOSIS_EXCESS =  4.0    # g₂: kurtosis exceso (normal=0, BTC empirico ~4-7)

# ── Filtros de mercado ────────────────────────────────────────────────────────
MIN_MARKET_VOLUME_USD = 100_000  # Solo mercados con suficiente liquidez
MIN_MARKET_DAYS       = 0        # Acepta mercados diarios (ends hoy o manana)
MAX_MARKET_DAYS       = 2        # No operar en mercados de mas de 2 dias

# ── Edge y timing ─────────────────────────────────────────────────────────────
MIN_EDGE                      = 0.09   # overrideable por env MIN_EDGE_BTC
BLACKOUT_MINUTES_BEFORE_CLOSE = 10
MIN_HOURS_ELAPSED             = 2.0   # no operar primeras 2h (alta incertidumbre)

# ── Funding rate (solo ajuste en extremos) ───────────────────────────────────
FUNDING_EXTREME_LONG  =  0.0005   # +0.05%/8h: mercado muy sobre-comprado
FUNDING_EXTREME_SHORT = -0.0003   # -0.03%/8h: mercado muy sobre-vendido
FUNDING_ADJ_EXTREME   =  0.03     # ajuste de 3pp cuando funding es extremo
FUNDING_CACHE_TTL     = 3600      # actualizar cada hora

# ── Confirmacion de entrada ───────────────────────────────────────────────────
MIN_CONSECUTIVE_SIGNALS = 2

# ── Intervalos de polling/cache ───────────────────────────────────────────────
EVAL_INTERVAL_SECS = 60        # evaluar cada 60s
PRICE_POLL_SECS    = 10        # precio BTC cada 10s (REST fallback)
VOL_CACHE_TTL      = 3600      # recalcular HAR-RV cada hora
MARKET_CACHE_TTL   = 30        # cache de mercado activo 30s
MOVE_TRIGGER_PCT   = 0.003     # disparar evaluacion si BTC mueve 0.3%


# ── Implementacion N(x) sin scipy ────────────────────────────────────────────
def _norm_cdf(x: float) -> float:
    """CDF de la normal estandar via aproximacion de Abramowitz & Stegun (error < 1.5e-7)."""
    sign = 1.0 if x >= 0 else -1.0
    x = abs(x)
    t = 1.0 / (1.0 + 0.2316419 * x)
    p = t * (0.319381530 + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))))
    return 0.5 + sign * (0.5 - math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi) * p)


class BtcArbMonitor:
    """
    Monitor de arbitraje para mercados binarios de BTC en Polymarket.
    Compara la probabilidad real N(d₂)+Cornish-Fisher con el precio del mercado.
    Solo opera en mercados DIARIOS con alto volumen para evitar fees dinamicos.
    """

    def __init__(
        self,
        executor=None,
        notifier=None,
        pnl_tracker=None,          # mantenido por compatibilidad, no se usa
        min_edge: float = MIN_EDGE,
        dry_run: bool = True,
        bankroll_usd: float = 100.0,
        proxy: str = None,
        alchemy_api_key: str = None,
    ):
        self.executor     = executor
        self.notifier     = notifier
        self.min_edge     = min_edge
        self.dry_run      = dry_run
        self.bankroll_usd = bankroll_usd
        # proxy y alchemy no se usan en v4 (Chainlink descontinuado como estrategia)

        # Precio BTC
        self._btc_price: float = 0.0
        self._price_lock       = threading.Lock()
        self._last_eval_price  = 0.0

        # State
        self._running          = False
        self._market_cache: Optional[dict] = None
        self._market_cache_ts  = 0.0

        # Daily reset — protegido con lock para thread-safety
        self._exec_lock        = threading.Lock()
        self._executed_today: set = set()
        self._last_exec_date   = None

        # HAR-RV volatilidad
        self._har_vol: float   = BTC_DAILY_VOL_FALLBACK
        self._vol_cache_ts     = 0.0

        # Funding rate
        self._funding_rate     = 0.0
        self._funding_cache_ts = 0.0

        # Confirmacion de entrada
        self._consecutive_signal  = 0
        self._pending_condition_id = ""
        self._eval_lock            = threading.Lock()

        # Trigger para evaluacion event-driven
        self._trigger_eval = threading.Event()

    # ── Public API ──────────────────────────────────────────────────────────

    def start(self):
        self._running = True
        threading.Thread(target=self._price_poller,   daemon=True, name="btc-price").start()
        threading.Thread(target=self._eval_loop,      daemon=True, name="btc-eval").start()
        threading.Thread(target=self._vol_updater,    daemon=True, name="btc-vol").start()
        threading.Thread(target=self._funding_updater, daemon=True, name="btc-fund").start()
        logger.info("[BTC ARB v4] Monitor iniciado - N(d2)+Cornish-Fisher+HAR-RV activo")

    def stop(self):
        self._running = False
        self._trigger_eval.set()

    @property
    def btc_price(self) -> float:
        with self._price_lock:
            return self._btc_price

    # ── Precio BTC via REST polling ─────────────────────────────────────────

    def _price_poller(self):
        """Obtiene precio BTC via REST cada 10s y dispara evaluacion si mueve 0.3%."""
        while self._running:
            try:
                r = requests.get(
                    BINANCE_REST_TICKER,
                    params={"symbol": "BTCUSDT"},
                    timeout=5,
                )
                price = float(r.json().get("price", 0))
                if price > 0:
                    with self._price_lock:
                        self._btc_price = price
                    if self._last_eval_price > 0:
                        move = abs(price - self._last_eval_price) / self._last_eval_price
                        if move >= MOVE_TRIGGER_PCT:
                            self._trigger_eval.set()
            except Exception as e:
                logger.debug(f"[BTC ARB] Price poll error: {e}")
            time.sleep(PRICE_POLL_SECS)

    # ── Evaluation Loop ─────────────────────────────────────────────────────

    def _eval_loop(self):
        """Evalua cada 60s o cuando BTC mueve >0.3%."""
        # Esperar precio inicial
        for _ in range(30):
            if self.btc_price > 0:
                break
            time.sleep(1)

        while self._running:
            triggered = self._trigger_eval.wait(timeout=EVAL_INTERVAL_SECS)
            self._trigger_eval.clear()
            if not self._running:
                break
            try:
                with self._eval_lock:
                    self._evaluate(triggered_by_move=triggered)
            except Exception as e:
                logger.error(f"[BTC ARB] Eval error: {e}", exc_info=True)

    # ── HAR-RV Volatility ───────────────────────────────────────────────────

    def _vol_updater(self):
        """Actualiza HAR-RV cada hora."""
        while self._running:
            vol = self._update_har_vol()
            if vol > 0:
                self._har_vol    = vol
                self._vol_cache_ts = time.time()
                logger.info(f"[BTC ARB] HAR-RV: sigma={vol:.4f} ({vol*100:.2f}%/dia)")
            time.sleep(VOL_CACHE_TTL)

    def _update_har_vol(self) -> float:
        """
        Calcula HAR-RV (Corsi 2009) usando velas de 5 minutos de Binance.

        HAR-RV: σ_t = β₀ + β_d·RV_1d + β_w·RV_5d + β_m·RV_22d
        RV = sqrt(suma de retornos^2 sobre el periodo), anualizado a diario.

        Ventaja sobre vol diaria: captura la estructura de correlacion
        de la volatilidad a multiples horizontes (dia, semana, mes).
        """
        try:
            now      = datetime.now(timezone.utc)
            end_ts   = int(now.timestamp() * 1000)
            # 22 dias de velas de 5min = ~22*288 = 6336 velas
            start_ts = int((now - timedelta(days=23)).timestamp() * 1000)

            r = requests.get(
                BINANCE_KLINES,
                params={
                    "symbol":    "BTCUSDT",
                    "interval":  "5m",
                    "startTime": start_ts,
                    "endTime":   end_ts,
                    "limit":     1500,   # maximo por request
                },
                timeout=15,
            )
            candles = r.json()
            if len(candles) < 50:
                logger.warning(f"[BTC ARB] HAR-RV: pocas velas ({len(candles)}), usando fallback")
                return BTC_DAILY_VOL_FALLBACK

            # Retornos de 5 minutos
            closes  = [float(c[4]) for c in candles]
            ret5m   = [(closes[i] - closes[i-1]) / closes[i-1] for i in range(1, len(closes))]

            # Velas por dia: 288 (24h * 12 per hora)
            BARS_PER_DAY = 288

            def rv_period(returns_list: list) -> float:
                """RV de un periodo = sqrt(suma r^2), convertido a escala diaria."""
                if not returns_list:
                    return BTC_DAILY_VOL_FALLBACK
                ss = sum(r * r for r in returns_list)
                # rv intraday → escalar a diario: * sqrt(BARS_PER_DAY / len)
                scale = (BARS_PER_DAY / len(returns_list)) ** 0.5
                return (ss ** 0.5) * scale

            # RV componente diaria (ultimo dia)
            rv_1d = rv_period(ret5m[-BARS_PER_DAY:])

            # RV componente semanal (5 dias)
            rv_5d = rv_period(ret5m[-5 * BARS_PER_DAY:])

            # RV componente mensual (22 dias)
            rv_22d = rv_period(ret5m[-22 * BARS_PER_DAY:]) if len(ret5m) >= 22 * BARS_PER_DAY else rv_5d

            har_vol = HAR_INTERCEPT + HAR_BETA_DAILY * rv_1d + HAR_BETA_WEEKLY * rv_5d + HAR_BETA_MONTHLY * rv_22d

            # Clamp: min 0.5%/dia, max 10%/dia
            return max(0.005, min(0.10, har_vol))

        except Exception as e:
            logger.warning(f"[BTC ARB] HAR-RV error: {e}")
            return BTC_DAILY_VOL_FALLBACK

    # ── Funding Rate ────────────────────────────────────────────────────────

    def _funding_updater(self):
        """Actualiza funding rate cada hora."""
        while self._running:
            rate = self._fetch_funding_rate()
            if rate is not None:
                self._funding_rate     = rate
                self._funding_cache_ts = time.time()
                logger.debug(f"[BTC ARB] Funding: {rate:+.6f}/8h")
            time.sleep(FUNDING_CACHE_TTL)

    def _fetch_funding_rate(self) -> Optional[float]:
        try:
            r = requests.get(
                BINANCE_FUNDING,
                params={"symbol": "BTCUSDT", "limit": 1},
                timeout=10,
            )
            data = r.json()
            if data:
                return float(data[-1]["fundingRate"])
        except Exception as e:
            logger.debug(f"[BTC ARB] Funding fetch error: {e}")
        return None

    # ── Core: N(d₂) + Cornish-Fisher ───────────────────────────────────────

    def _calc_probability(self, btc_spot: float, strike: float, hours_left: float, sigma_daily: float) -> float:
        """
        Calcula P(BTC_final > K) usando la formula correcta de opcion digital cash-or-nothing.

        Formula Black-Scholes para opcion digital:
            d₂ = [ln(S/K) - (σ²/2)·T] / (σ·√T)
            P(S_T > K) = N(d₂)

        Correccion Cornish-Fisher para fat tails de BTC:
            z_CF = d₂ - (g₁/6)·(d₂²-1) - (g₂/24)·(d₂³-3d₂) + (g₁²/36)·(2d₂³-5d₂)
            P_corr = N(z_CF)

        Donde:
            S = precio spot actual de BTC
            K = strike (precio de referencia del mercado)
            T = tiempo restante en dias
            σ = volatilidad diaria (HAR-RV)
            g₁ = skewness de retornos BTC (~-0.2)
            g₂ = kurtosis excess de retornos BTC (~4.0)

        Fundamento: los fat tails de BTC (kurtosis real ~6-10) hacen que N(d₂)
        puro SUBESTIME las probabilidades extremas. Cornish-Fisher ajusta d₂
        para reflejar la distribucion real sin requerir implementar el modelo Kou
        completo de saltos.
        """
        if strike <= 0 or sigma_daily <= 0 or hours_left <= 0:
            return 0.5

        T = hours_left / 24.0  # tiempo en dias

        # d₂ de Black-Scholes
        try:
            log_moneyness = math.log(btc_spot / strike)
        except ValueError:
            return 0.5

        sigma_t = sigma_daily * math.sqrt(T)
        if sigma_t < 1e-8:
            # Tiempo casi cero: resultado determinista
            return 1.0 if btc_spot > strike else 0.0

        d2 = (log_moneyness - 0.5 * sigma_daily ** 2 * T) / sigma_t

        # Cornish-Fisher: ajustar d₂ por skewness y kurtosis
        g1 = BTC_SKEWNESS
        g2 = BTC_KURTOSIS_EXCESS
        d2_sq = d2 * d2
        d2_cb = d2_sq * d2

        z_cf = (
            d2
            - (g1 / 6.0)  * (d2_sq - 1.0)
            - (g2 / 24.0) * (d2_cb - 3.0 * d2)
            + (g1 ** 2 / 36.0) * (2.0 * d2_cb - 5.0 * d2)
        )

        p = _norm_cdf(z_cf)

        # Ajuste de funding rate — solo en extremos de posicionamiento
        funding = self._funding_rate
        if funding > FUNDING_EXTREME_LONG:
            # Mercado muy sobre-comprado → presion bajista real
            p = max(0.01, p - FUNDING_ADJ_EXTREME)
        elif funding < FUNDING_EXTREME_SHORT:
            # Mercado muy sobre-vendido → presion alcista real
            p = min(0.99, p + FUNDING_ADJ_EXTREME)

        return max(0.01, min(0.99, p))

    # ── Evaluate ────────────────────────────────────────────────────────────

    def _evaluate(self, triggered_by_move: bool = False):
        btc = self.btc_price
        if btc <= 0:
            return

        self._last_eval_price = btc

        # Reset diario — thread-safe
        today = datetime.now(timezone.utc).date()
        with self._exec_lock:
            if self._last_exec_date != today:
                self._executed_today.clear()
                self._last_exec_date = today

        market = self._get_active_market()
        if not market:
            if not triggered_by_move:
                logger.debug(f"[BTC ARB] BTC=${btc:,.0f} | sin mercado diario activo")
            return

        condition_id = market.get("conditionId", "")
        with self._exec_lock:
            if condition_id in self._executed_today:
                return

        strike = self._get_strike(market)
        if not strike:
            logger.debug(f"[BTC ARB] Sin strike para: {market.get('question', '')[:50]}")
            return

        end_dt = self._parse_end_date(market.get("endDate", ""))
        if not end_dt:
            return

        now          = datetime.now(timezone.utc)
        hours_left   = (end_dt - now).total_seconds() / 3600
        hours_elapsed = 24.0 - hours_left

        if hours_left <= 0:
            return
        if hours_left < BLACKOUT_MINUTES_BEFORE_CLOSE / 60.0:
            logger.debug("[BTC ARB] Blackout — muy cerca del cierre")
            return
        if hours_elapsed < MIN_HOURS_ELAPSED:
            logger.debug(f"[BTC ARB] Muy temprano ({hours_elapsed:.1f}h transcurridas)")
            return

        # Calcular probabilidad con formula correcta
        sigma = self._har_vol
        p_true_up = self._calc_probability(btc, strike, hours_left, sigma)
        p_true_dn = 1.0 - p_true_up

        # Precio del mercado
        try:
            raw = market.get("outcomePrices", "[0.5,0.5]")
            prices_list = json.loads(raw) if isinstance(raw, str) else raw
            p_mkt_up = float(prices_list[0])
            p_mkt_dn = float(prices_list[1])
        except Exception:
            return

        # El edge es la diferencia entre probabilidad real y precio de mercado, menos fee
        fee = 0.02
        edge_up = p_true_up - p_mkt_up - fee
        edge_dn = p_true_dn - p_mkt_dn - fee

        # Tomar el lado con mayor edge neto positivo
        if edge_up >= edge_dn and edge_up > 0:
            best_edge, side, p_true, p_mkt = edge_up, "UP",   p_true_up, p_mkt_up
        elif edge_dn > edge_up and edge_dn > 0:
            best_edge, side, p_true, p_mkt = edge_dn, "DOWN", p_true_dn, p_mkt_dn
        else:
            best_edge, side, p_true, p_mkt = max(edge_up, edge_dn), "UP" if edge_up >= edge_dn else "DOWN", p_true_up, p_mkt_up

        move_pct = (btc - strike) / strike
        trigger_tag = "MOVE" if triggered_by_move else "TIMER"
        log_fn = logger.info if triggered_by_move or best_edge >= self.min_edge else logger.debug
        log_fn(
            f"[BTC ARB/{trigger_tag}] BTC=${btc:,.0f} K=${strike:,.0f} move={move_pct:+.2%} | "
            f"sigma_HAR={sigma:.4f} T={hours_left:.1f}h | "
            f"P(UP) real={p_true_up:.3f} mkt={p_mkt_up:.3f} | "
            f"edge_{side}={best_edge:+.4f} (min={self.min_edge})"
        )

        if best_edge < self.min_edge:
            # Reset confirmacion si edge cae
            if self._pending_condition_id:
                self._consecutive_signal  = 0
                self._pending_condition_id = ""
            return

        # Entry confirmation: N evaluaciones consecutivas
        if self._pending_condition_id != condition_id:
            self._consecutive_signal  = 0
            self._pending_condition_id = condition_id

        self._consecutive_signal += 1

        if self._consecutive_signal < MIN_CONSECUTIVE_SIGNALS:
            logger.info(
                f"[BTC ARB] Confirmacion {self._consecutive_signal}/{MIN_CONSECUTIVE_SIGNALS} — "
                f"edge={best_edge:+.4f} | esperando proxima evaluacion..."
            )
            return

        # Confirmado — ejecutar y registrar
        self._consecutive_signal  = 0
        self._pending_condition_id = ""
        self._execute_signal(
            market=market, side=side, p_true=p_true, p_mkt=p_mkt,
            edge=best_edge, btc_price=btc, strike=strike, hours_left=hours_left,
        )
        with self._exec_lock:
            self._executed_today.add(condition_id)

    # ── Execute Signal ──────────────────────────────────────────────────────

    def _execute_signal(self, market, side, p_true, p_mkt, edge, btc_price, strike, hours_left):
        question     = market.get("question", "BTC Up or Down")
        condition_id = market.get("conditionId", "")

        if not self.executor:
            mode = "DRY RUN (sin executor)"
            logger.info(
                f"[BTC ARB {mode}] {question[:60]} | "
                f"side={side} price={p_mkt:.3f} fair={p_true:.3f} edge={edge:+.4f}"
            )
            if self.notifier:
                self.notifier._send(
                    f"[BTC ARB] {question[:80]}\n"
                    f"Side: {side} @ {p_mkt:.3f} | fair={p_true:.3f} | edge={edge:+.4f}\n"
                    f"BTC: ${btc_price:,.0f} vs K=${strike:,.0f} | {hours_left:.1f}h restantes\n"
                    f"sigma_HAR={self._har_vol:.4f} | fund={self._funding_rate:+.6f}"
                )
            return

        from src.signals.base import Opportunity, SignalType
        token_ids = []
        try:
            token_ids = json.loads(market.get("clobTokenIds", "[]"))
        except Exception:
            pass

        token_id = (
            token_ids[0] if side == "UP"   and len(token_ids) > 0 else
            token_ids[1] if side == "DOWN" and len(token_ids) > 1 else ""
        )

        # UP compra el token YES (index 0), DOWN compra el token NO (index 1).
        # El P&L tracker solo entiende YES/NO para calcular correctamente el resultado.
        clob_side = "YES" if side == "UP" else "NO"

        opp = Opportunity(
            signal_type   = SignalType.PRICE_DRIFT,
            condition_id  = condition_id,
            token_id      = token_id,
            question      = question,
            category      = "Crypto",
            side          = clob_side,
            market_price  = p_mkt,
            fair_value    = p_true,
            edge          = edge,
            edge_pct      = edge / p_mkt if p_mkt > 0 else 0,
            best_bid      = p_mkt,
            best_ask      = p_mkt,
            spread        = 0.001,
            liquidity_usd = float(market.get("liquidityNum") or 0),
            volume_24h    = float(market.get("volume24hr") or 0),
            notes=(
                f"BTC_ARB_v4 | N(d2)+CF | "
                f"btc={btc_price:.0f} K={strike:.0f} "
                f"sigma={self._har_vol:.4f} T={hours_left:.1f}h "
                f"fund={self._funding_rate:+.6f}"
            ),
        )

        result = self.executor.maybe_execute(opp)
        if result:
            mode = "DRY RUN" if result.dry_run else "TRADE"
            msg = (
                f"[BTC ARB {mode}] {question[:80]}\n"
                f"Side: {side} @ {result.price:.3f} | ${result.position_usd:.2f} | edge={edge:+.4f}\n"
                f"BTC: ${btc_price:,.0f} vs K=${strike:,.0f} | {hours_left:.1f}h left"
            )
            if not result.dry_run:
                msg += f"\nOrder ID: {result.order_id}"
            if self.notifier:
                self.notifier._send(msg)

    # ── Helpers ─────────────────────────────────────────────────────────────

    def _get_active_market(self) -> Optional[dict]:
        """
        Obtiene el mercado BTC diario activo con mayor volumen.
        Solo acepta mercados que resuelven en las proximas 48h (evitar fees dinamicos).
        Cache de 30s.
        """
        now_ts = time.time()
        if self._market_cache and now_ts - self._market_cache_ts < MARKET_CACHE_TTL:
            return self._market_cache

        try:
            r = requests.get(GAMMA_SEARCH_URL, timeout=10)
            markets = r.json()

            now_utc  = datetime.now(timezone.utc)
            btc      = self.btc_price
            candidates = []

            for m in markets:
                if m.get("closed", False):
                    continue

                q = m.get("question", "").lower()
                if "bitcoin" not in q and "btc" not in q:
                    continue

                # Verificar que tiene strike (precio de referencia)
                strike = self._get_strike(m)
                if not strike:
                    continue

                # Verificar horizonte temporal: solo mercados que resuelven en 0-48h
                end_dt = self._parse_end_date(m.get("endDate", ""))
                if not end_dt:
                    continue
                hours_to_end = (end_dt - now_utc).total_seconds() / 3600
                if hours_to_end <= 0 or hours_to_end > MAX_MARKET_DAYS * 24:
                    continue

                # Verificar volumen minimo
                vol = float(m.get("volume24hr") or 0)
                if vol < MIN_MARKET_VOLUME_USD:
                    continue

                candidates.append((m, strike, vol))

            if not candidates:
                self._market_cache    = None
                self._market_cache_ts = now_ts
                return None

            # Preferir el strike mas cercano al precio actual (maxima incertidumbre = mayor edge potencial)
            if btc > 0:
                candidates.sort(key=lambda x: (abs(x[1] - btc), -x[2]))
            else:
                candidates.sort(key=lambda x: -x[2])

            best_market, best_strike, best_vol = candidates[0]
            self._market_cache    = best_market
            self._market_cache_ts = now_ts
            logger.debug(
                f"[BTC ARB] Mercado: K=${best_strike:,.0f} vol=${best_vol:,.0f} "
                f"({best_market.get('question','')[:60]})"
            )
            return best_market

        except Exception as e:
            logger.warning(f"[BTC ARB] No se pudo obtener mercado: {e}")
            return None

    def _get_strike(self, market: dict) -> Optional[float]:
        """Extrae el precio de referencia (strike) del mercado."""
        # Primero: strike pre-parseado en cache
        if "_parsed_strike" in market:
            return market["_parsed_strike"]

        # Segundo: eventMetadata.priceToBeat (mercados legacy)
        try:
            events = market.get("events", [])
            if events:
                meta = events[0].get("eventMetadata", {})
                val = float(meta.get("priceToBeat", 0))
                if val > 0:
                    return val
        except Exception:
            pass

        # Tercero: parsear del titulo "above $X"
        return self._parse_strike_from_question(market.get("question", ""))

    def _parse_strike_from_question(self, question: str) -> Optional[float]:
        """Extrae strike de 'Will the price of Bitcoin be above $70,000 on...'"""
        import re
        m = re.search(r'\$([0-9,]+)', question)
        if m:
            try:
                return float(m.group(1).replace(",", ""))
            except ValueError:
                pass
        return None

    def _parse_end_date(self, s: str) -> Optional[datetime]:
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
        except Exception:
            return None
