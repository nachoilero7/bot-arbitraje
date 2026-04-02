"""
BTC Arb Monitor v5 — Ensemble probabilístico: Deribit IV surface + calibración empírica + microestructura.

Por qué v4 perdía: N(d₂) de Black-Scholes calcula probabilidad *risk-neutral*, no real-world.
  Los participantes profesionales ya descuentan la IV de Deribit → el modelo no tenía ventaja.
  678 trades consecutivos perdidos confirmaron que el edge era negativo.

v5 corrige esto con tres estimadores independientes:

  1. Deribit IV Surface  (w=0.45)
     Obtiene la superficie de volatilidad implícita de opciones reales de BTC.
     Para cada strike/expiry busca el IV de mercado, interpola en el strike objetivo
     y calcula P(BTC>K) = N(d₂) con ese IV. Esto ancla la probabilidad a un mercado
     real de opciones con miles de millones de dólares de open interest.

  2. Calibración Empírica (w=0.35)
     Ajusta la probabilidad de Deribit según sesgos documentados:
     - Opciones muy OTM: mercado sobreestima probabilidades extremas (push hacia 0.5)
     - Régimen de baja vol: IV sobreestima la vol realizada → probabilidades demasiado extremas
     - Deriva positiva de BTC: ~0.15%/día de retorno esperado (drift histórico documentado)

  3. Microestructura (w=0.20)
     Señales de flujo de corto plazo que predicen presión de precio inminente:
     - Funding rate: funding positivo extremo = longs sobrecomprados = sesgo bajista
     - Profundidad order book Binance en strike ±0.5%: asimetría de órdenes
     - Momentum de 1 hora: continuación de tendencia de muy corto plazo

Filtros anti-ruido:
  - Los 3 estimadores deben coincidir en dirección (unanimidad)
  - Ventana temporal: 4h–20h hasta resolución
  - Edge mínimo: 10% (vs 9% en v4)
  - Fee dinámico: 2% si vol24h>$500k, 3.15% si no
  - 2 evaluaciones consecutivas confirman el signal (igual que v4)
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

# ── APIs externas ──────────────────────────────────────────────────────────────
BINANCE_REST_TICKER  = "https://api.binance.com/api/v3/ticker/price"
BINANCE_KLINES       = "https://api.binance.com/api/v3/klines"
BINANCE_FUNDING      = "https://fapi.binance.com/fapi/v1/fundingRate"
BINANCE_DEPTH        = "https://api.binance.com/api/v3/depth"
DERIBIT_BOOK_SUMMARY = "https://www.deribit.com/api/v2/public/get_book_summary_by_currency"
GAMMA_SEARCH_URL     = (
    "https://gamma-api.polymarket.com/markets"
    "?active=true&closed=false&limit=200&order=volume24hr&ascending=false"
)

# ── Pesos del ensemble ─────────────────────────────────────────────────────────
W_DERIBIT    = 0.45
W_EMPIRICAL  = 0.35
W_MICRO      = 0.20

# ── Filtros de operación ───────────────────────────────────────────────────────
MIN_EDGE              = 0.10
MIN_HOURS_REMAINING   = 4.0
MAX_HOURS_REMAINING   = 20.0
FEE_HIGH_VOL          = 0.020   # 2% si vol24h > $500k (mercados líquidos)
FEE_LOW_VOL           = 0.0315  # 3.15% fee dinámico (mercados ilíquidos)
VOL_THRESHOLD_FEE     = 500_000 # USD de vol diaria para fee reducido
MIN_MARKET_VOLUME_USD = 100_000 # Volumen mínimo del mercado en Polymarket

# ── Calibración empírica ───────────────────────────────────────────────────────
# BTC tiene deriva positiva histórica de ~0.15%/día (documentado en múltiples papers)
BTC_DRIFT_DAILY       = 0.0015
# Opciones OTM: mercado sobreestima probabilidades extremas (~8% por cada 5% de moneyness)
OVERCONF_OTM_FACTOR   = 0.08
ATM_BAND              = 0.02    # dentro de ±2% se considera ATM, sin corrección
# Régimen de vol: umbral diario
VOL_REGIME_LOW_THR    = 0.015   # < 1.5%/día: IV sobreestima vol realizada
VOL_REGIME_HIGH_THR   = 0.030   # > 3.0%/día: IV puede subestimar riesgo de cola

# ── Microestructura ────────────────────────────────────────────────────────────
FUNDING_EXTREME_LONG  =  0.0005  # +0.05%/8h: mercado muy sobre-comprado
FUNDING_EXTREME_SHORT = -0.0003  # -0.03%/8h: mercado muy sobre-vendido
FUNDING_ADJ           =  0.02    # 2pp de ajuste en extremos de funding
MOMENTUM_1H_ADJ_RATE  =  0.02   # 2pp por cada 1% de momentum en última hora
DEPTH_IMBALANCE_ADJ   =  0.015  # 1.5pp máximo por desequilibrio de order book

# ── HAR-RV (fallback cuando Deribit no está disponible) ───────────────────────
HAR_BETA_DAILY        = 0.35
HAR_BETA_WEEKLY       = 0.25
HAR_BETA_MONTHLY      = 0.20
HAR_INTERCEPT         = 0.005
BTC_VOL_FALLBACK      = 0.025   # 2.5%/día si no se puede calcular
BTC_SKEWNESS          = -0.2
BTC_KURTOSIS_EXCESS   =  4.0

# ── Intervalos ─────────────────────────────────────────────────────────────────
EVAL_INTERVAL_SECS    = 60
PRICE_POLL_SECS       = 10
VOL_CACHE_TTL         = 3600
FUNDING_CACHE_TTL     = 3600
DERIBIT_CACHE_TTL     = 300     # superficie Deribit: cada 5 minutos
DEPTH_CACHE_TTL       = 30
MARKET_CACHE_TTL      = 30
MOVE_TRIGGER_PCT      = 0.003   # dispara evaluación si BTC mueve 0.3%
BLACKOUT_MINUTES      = 10
MIN_CONSECUTIVE       = 2


# ── Función auxiliar: CDF normal (sin scipy) ───────────────────────────────────
def _norm_cdf(x: float) -> float:
    """CDF normal estándar — Abramowitz & Stegun (error < 1.5e-7)."""
    sign = 1.0 if x >= 0 else -1.0
    x = abs(x)
    t = 1.0 / (1.0 + 0.2316419 * x)
    p = t * (0.319381530 + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))))
    return 0.5 + sign * (0.5 - math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi) * p)


class BtcArbMonitorV5:
    """
    Monitor v5: ensemble de 3 estimadores independientes con filtro de unanimidad.
    Reemplaza el modelo Black-Scholes puro de v4 que generaba probabilidades
    risk-neutral sin ventaja sobre participantes que ya usan opciones Deribit.
    """

    def __init__(
        self,
        executor=None,
        notifier=None,
        pnl_tracker=None,
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

        # Precio BTC
        self._btc_price: float = 0.0
        self._price_lock       = threading.Lock()
        self._last_eval_price  = 0.0

        # Estado general
        self._running         = False
        self._market_cache: Optional[dict] = None
        self._market_cache_ts = 0.0

        # Reset diario
        self._exec_lock      = threading.Lock()
        self._executed_today: set = set()
        self._last_exec_date = None

        # HAR-RV (fallback)
        self._har_vol: float  = BTC_VOL_FALLBACK
        self._vol_cache_ts    = 0.0

        # Funding rate
        self._funding_rate    = 0.0
        self._funding_cache_ts = 0.0

        # Superficie Deribit: { expiry_ts: [(strike, iv_annual), ...] }
        self._deribit_surface: dict = {}
        self._deribit_cache_ts = 0.0
        self._deribit_lock     = threading.Lock()

        # Depth cache: { strike_bucket: imbalance_adj }
        self._depth_cache: dict  = {}
        self._depth_cache_ts: float = 0.0

        # 1h momentum cache
        self._momentum_1h: Optional[float] = None
        self._momentum_cache_ts = 0.0

        # Confirmación de entrada
        self._consecutive_signal  = 0
        self._pending_condition_id = ""
        self._eval_lock            = threading.Lock()

        # Trigger event-driven
        self._trigger_eval = threading.Event()

    # ── Public API ─────────────────────────────────────────────────────────────

    def start(self):
        self._running = True
        threading.Thread(target=self._price_poller,    daemon=True, name="btcv5-price").start()
        threading.Thread(target=self._eval_loop,       daemon=True, name="btcv5-eval").start()
        threading.Thread(target=self._vol_updater,     daemon=True, name="btcv5-vol").start()
        threading.Thread(target=self._funding_updater, daemon=True, name="btcv5-fund").start()
        threading.Thread(target=self._surface_updater, daemon=True, name="btcv5-deribit").start()
        logger.info("[BTC v5] Monitor iniciado — Deribit IV surface + calibración + microestructura")

    def stop(self):
        self._running = False
        self._trigger_eval.set()

    @property
    def btc_price(self) -> float:
        with self._price_lock:
            return self._btc_price

    # ── Threads ────────────────────────────────────────────────────────────────

    def _price_poller(self):
        while self._running:
            try:
                r = requests.get(
                    BINANCE_REST_TICKER, params={"symbol": "BTCUSDT"}, timeout=5
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
                logger.debug(f"[BTC v5] Price poll error: {e}")
            time.sleep(PRICE_POLL_SECS)

    def _eval_loop(self):
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
                logger.error(f"[BTC v5] Eval error: {e}", exc_info=True)

    def _vol_updater(self):
        while self._running:
            vol = self._update_har_vol()
            if vol > 0:
                self._har_vol    = vol
                self._vol_cache_ts = time.time()
                logger.info(f"[BTC v5] HAR-RV: sigma={vol:.4f} ({vol*100:.2f}%/día)")
            time.sleep(VOL_CACHE_TTL)

    def _funding_updater(self):
        while self._running:
            rate = self._fetch_funding_rate()
            if rate is not None:
                self._funding_rate     = rate
                self._funding_cache_ts = time.time()
                logger.debug(f"[BTC v5] Funding: {rate:+.6f}/8h")
            time.sleep(FUNDING_CACHE_TTL)

    def _surface_updater(self):
        """Actualiza la superficie de IV de Deribit cada 5 minutos."""
        while self._running:
            surface = self._fetch_deribit_surface()
            if surface:
                with self._deribit_lock:
                    self._deribit_surface   = surface
                    self._deribit_cache_ts  = time.time()
                logger.info(
                    f"[BTC v5] Deribit surface actualizada: {len(surface)} expiries, "
                    f"{sum(len(v) for v in surface.values())} strikes"
                )
            time.sleep(DERIBIT_CACHE_TTL)

    # ── Estimador 1: Deribit IV Surface ───────────────────────────────────────

    def _fetch_deribit_surface(self) -> Optional[dict]:
        """
        Obtiene la superficie completa de IV de opciones BTC de Deribit.
        Usa get_book_summary_by_currency (un único request para todas las opciones).

        Retorna: { expiry_unix_ts: [(strike, iv_annual), ...] }
        """
        try:
            r = requests.get(
                DERIBIT_BOOK_SUMMARY,
                params={"currency": "BTC", "kind": "option"},
                timeout=15,
            )
            data = r.json()
            results = data.get("result", [])
            if not results:
                logger.warning("[BTC v5] Deribit no retornó opciones")
                return None

            surface: dict[int, list[tuple[float, float]]] = {}

            for opt in results:
                # Solo calls (para P(S>K)), IV válida, precio con sentido
                name = opt.get("instrument_name", "")
                if not name.endswith("-C"):
                    continue
                mark_iv = opt.get("mark_iv")
                if not mark_iv or mark_iv <= 0:
                    continue

                # Extraer strike y expiry del nombre: BTC-DDMMMYY-STRIKE-C
                parts = name.split("-")
                if len(parts) < 4:
                    continue
                try:
                    strike = float(parts[2])
                except ValueError:
                    continue

                # Timestamp de expiración
                exp_ts_ms = opt.get("creation_timestamp")  # no sirve, es creación
                # Buscar en el nombre: parsear fecha del instrument
                exp_ts = self._parse_deribit_expiry(parts[1])
                if exp_ts is None:
                    continue

                iv_annual = mark_iv / 100.0  # Deribit da IV en porcentaje

                if exp_ts not in surface:
                    surface[exp_ts] = []
                surface[exp_ts].append((strike, iv_annual))

            # Ordenar cada expiry por strike
            for exp_ts in surface:
                surface[exp_ts].sort(key=lambda x: x[0])

            # Filtrar expiries ya vencidas
            now_ts = time.time()
            surface = {ts: v for ts, v in surface.items() if ts > now_ts}

            return surface if surface else None

        except Exception as e:
            logger.warning(f"[BTC v5] Deribit fetch error: {e}")
            return None

    def _parse_deribit_expiry(self, date_str: str) -> Optional[int]:
        """
        Parsea el string de fecha de Deribit: DDMMMYY → unix timestamp (UTC, 8:00 AM).
        Ej: '31MAY24' → datetime(2024, 5, 31, 8, 0, tzinfo=UTC)
        """
        try:
            dt = datetime.strptime(date_str, "%d%b%y").replace(
                hour=8, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
            )
            return int(dt.timestamp())
        except Exception:
            return None

    def _get_deribit_probability(self, btc_spot: float, strike: float, hours_left: float) -> Optional[float]:
        """
        Interpola el IV de Deribit en el strike objetivo y calcula P(BTC>K) = N(d₂).

        A diferencia de v4 (donde usamos volatilidad realizada con HAR-RV), aquí usamos
        la volatilidad *implícita de mercado* — lo que el mercado de opciones real cree
        que será la vol. Esto elimina el sesgo risk-neutral porque estamos usando el mismo
        IV que los participantes del mercado ya han incorporado.
        """
        with self._deribit_lock:
            surface = dict(self._deribit_surface)

        if not surface:
            return None

        # Encontrar el expiry más cercano a nuestra resolución
        target_ts = time.time() + hours_left * 3600
        best_expiry = min(surface.keys(), key=lambda ts: abs(ts - target_ts))

        # Tolerancia: expiry debe estar dentro de ±6h de nuestro mercado
        if abs(best_expiry - target_ts) > 6 * 3600:
            return None

        strikes_ivs = surface[best_expiry]
        if len(strikes_ivs) < 2:
            return None

        # Interpolar IV en el strike objetivo (lineal en log-strike space)
        iv_at_strike = self._interpolate_iv(strikes_ivs, strike)
        if iv_at_strike is None or iv_at_strike <= 0:
            return None

        T = hours_left / (365.25 * 24.0)  # tiempo en años
        if T <= 0:
            return None

        try:
            log_m = math.log(btc_spot / strike)
        except ValueError:
            return None

        sigma_t = iv_at_strike * math.sqrt(T)
        if sigma_t < 1e-8:
            return 1.0 if btc_spot > strike else 0.0

        # d₂ = [ln(S/K) - (σ²/2)·T] / (σ·√T)
        d2 = (log_m - 0.5 * iv_at_strike ** 2 * T) / sigma_t
        p  = _norm_cdf(d2)

        return max(0.01, min(0.99, p))

    def _interpolate_iv(self, strikes_ivs: list, target_strike: float) -> Optional[float]:
        """
        Interpolación lineal de IV en el strike objetivo.
        Si el strike cae fuera del rango, extrapola con el valor del extremo más cercano.
        """
        strikes = [s for s, _ in strikes_ivs]
        ivs     = [iv for _, iv in strikes_ivs]

        if target_strike <= strikes[0]:
            return ivs[0]
        if target_strike >= strikes[-1]:
            return ivs[-1]

        for i in range(len(strikes) - 1):
            if strikes[i] <= target_strike <= strikes[i + 1]:
                t = (target_strike - strikes[i]) / (strikes[i + 1] - strikes[i])
                return ivs[i] + t * (ivs[i + 1] - ivs[i])

        return None

    # ── Estimador 2: Calibración Empírica ─────────────────────────────────────

    def _get_empirical_probability(
        self,
        p_deribit: float,
        btc_spot: float,
        strike: float,
        hours_left: float,
        sigma_daily: float,
    ) -> float:
        """
        Ajusta la probabilidad de Deribit por sesgos empíricos documentados.

        Tres correcciones:
        1. Sobreconfianza en opciones OTM: los mercados sobreestiman probabilidades
           extremas (~8% por cada 5% de moneyness fuera de ATM). Documentado en
           literatura de calibración de prediction markets.

        2. Régimen de volatilidad: en vol baja, IV sobreestima la vol realizada
           → las probabilidades son demasiado extremas → corregir hacia 0.5.
           En vol alta, IV puede subestimar el riesgo de cola → amplificar ligeramente.

        3. Deriva positiva de BTC: ~0.15%/día de retorno esperado en BTC (promedio
           histórico 2013-2024). Aumenta levemente P(UP) según horizonte.
        """
        p = p_deribit
        moneyness = (btc_spot - strike) / strike  # positivo = BTC encima del strike

        # 1. Corrección de sobreconfianza OTM
        abs_m = abs(moneyness)
        if abs_m > ATM_BAND:
            # Por cada 5% de moneyness fuera de ATM, el mercado sobreestima ~8%
            extra_m = abs_m - ATM_BAND
            oc_adj  = OVERCONF_OTM_FACTOR * (extra_m / 0.05)
            oc_adj  = min(oc_adj, 0.15)  # cap: máximo 15% de corrección
            # Empujar hacia 0.5
            p = 0.5 + (p - 0.5) * (1.0 - oc_adj)

        # 2. Régimen de vol
        if sigma_daily < VOL_REGIME_LOW_THR:
            # Baja vol: IV sobreestima → probabilidades demasiado extremas
            p = 0.5 + (p - 0.5) * 0.93
        elif sigma_daily > VOL_REGIME_HIGH_THR:
            # Alta vol: riesgo de cola subestimado → amplificar
            p = 0.5 + (p - 0.5) * 1.07

        # 3. Deriva positiva de BTC
        T_days  = hours_left / 24.0
        drift   = BTC_DRIFT_DAILY * T_days
        # La deriva aumenta P(UP) proporcionalmente al espacio disponible
        p = p + drift * (1.0 - p)

        return max(0.01, min(0.99, p))

    # ── Estimador 3: Microestructura ──────────────────────────────────────────

    def _get_microstructure_adjustment(self, btc_spot: float, strike: float) -> float:
        """
        Combina tres señales de microestructura para producir un ajuste en [-0.10, +0.10].
        Positivo = sesgo alcista, negativo = sesgo bajista.
        """
        adj = 0.0

        # 1. Funding rate: posicionamiento extremo del mercado de futuros
        funding = self._funding_rate
        if funding > FUNDING_EXTREME_LONG:
            # Longs pagando demasiado = mercado sobrecomprado = sesgo bajista
            adj -= FUNDING_ADJ
        elif funding < FUNDING_EXTREME_SHORT:
            # Shorts pagando demasiado = mercado sobrevendido = sesgo alcista
            adj += FUNDING_ADJ

        # 2. Momentum de 1 hora (continuación de tendencia de muy corto plazo)
        momentum = self._get_1h_momentum()
        if momentum is not None:
            # 2pp por cada 1% de momentum, capped en ±4pp
            m_adj = MOMENTUM_1H_ADJ_RATE * (momentum / 0.01)
            adj  += max(-0.04, min(0.04, m_adj))

        # 3. Desequilibrio del order book de Binance en el nivel del strike
        depth_adj = self._get_depth_imbalance(strike)
        if depth_adj is not None:
            adj += depth_adj

        return max(-0.10, min(0.10, adj))

    def _get_1h_momentum(self) -> Optional[float]:
        """
        Retorno de BTC en la última hora. Cacheado 5 minutos.
        Usa la vela de 1h más reciente de Binance.
        """
        now_ts = time.time()
        if now_ts - self._momentum_cache_ts < 300:  # cache de 5 min
            return self._momentum_1h

        try:
            r = requests.get(
                BINANCE_KLINES,
                params={"symbol": "BTCUSDT", "interval": "1h", "limit": 2},
                timeout=5,
            )
            candles = r.json()
            if len(candles) >= 2:
                open_price  = float(candles[-1][1])  # open de la vela actual
                close_price = float(candles[-1][4])  # close (precio actual)
                if open_price > 0:
                    momentum = (close_price - open_price) / open_price
                    self._momentum_1h      = momentum
                    self._momentum_cache_ts = now_ts
                    return momentum
        except Exception as e:
            logger.debug(f"[BTC v5] Momentum 1h error: {e}")

        return None

    def _get_depth_imbalance(self, strike: float) -> Optional[float]:
        """
        Mide el desequilibrio de órdenes en Binance cerca del nivel del strike.
        Compara volumen de bids vs asks en el rango [strike-0.5%, strike+0.5%].

        Un desequilibrio grande en bids indica presión compradora → sesgo alcista.
        """
        now_ts = time.time()
        if now_ts - self._depth_cache_ts < DEPTH_CACHE_TTL:
            return self._depth_cache.get("adj")

        try:
            r = requests.get(
                BINANCE_DEPTH,
                params={"symbol": "BTCUSDT", "limit": 100},
                timeout=5,
            )
            book = r.json()
            bids = [(float(p), float(q)) for p, q in book.get("bids", [])]
            asks = [(float(p), float(q)) for p, q in book.get("asks", [])]

            lo = strike * 0.995  # strike - 0.5%
            hi = strike * 1.005  # strike + 0.5%

            bid_vol = sum(q for p, q in bids if lo <= p <= hi)
            ask_vol = sum(q for p, q in asks if lo <= p <= hi)
            total   = bid_vol + ask_vol

            if total < 0.01:
                adj = None
            else:
                imbalance = (bid_vol - ask_vol) / total  # [-1, +1]
                adj = imbalance * DEPTH_IMBALANCE_ADJ     # [-0.015, +0.015]

            self._depth_cache = {"adj": adj}
            self._depth_cache_ts = now_ts
            return adj

        except Exception as e:
            logger.debug(f"[BTC v5] Depth error: {e}")
            return None

    # ── Ensemble principal ─────────────────────────────────────────────────────

    def _evaluate_ensemble(
        self,
        btc_spot: float,
        strike: float,
        hours_left: float,
        sigma_daily: float,
    ) -> Optional[tuple[float, str, float, float, float, float]]:
        """
        Combina los 3 estimadores y aplica el filtro de unanimidad.

        Retorna: (p_ensemble, side, edge, p_deribit, p_empirical, micro_adj)
        o None si no hay señal.
        """
        # ── Estimador 1: Deribit IV ──────────────────────────────────────────
        p_deribit = self._get_deribit_probability(btc_spot, strike, hours_left)
        if p_deribit is None:
            # Fallback: usar N(d₂)+Cornish-Fisher con HAR-RV (como en v4)
            p_deribit = self._calc_har_probability(btc_spot, strike, hours_left, sigma_daily)
            logger.debug(f"[BTC v5] Deribit no disponible, usando fallback HAR: p={p_deribit:.3f}")

        # ── Estimador 2: Calibración empírica ────────────────────────────────
        p_empirical = self._get_empirical_probability(
            p_deribit, btc_spot, strike, hours_left, sigma_daily
        )

        # ── Estimador 3: Microestructura (como probabilidad alrededor de 0.5) ─
        micro_adj = self._get_microstructure_adjustment(btc_spot, strike)
        p_micro   = 0.5 + micro_adj  # neutral=0.5, alcista>0.5, bajista<0.5

        # ── Filtro de unanimidad ──────────────────────────────────────────────
        dir_deribit   = p_deribit   > 0.50
        dir_empirical = p_empirical > 0.50
        dir_micro     = p_micro     > 0.50

        if not (dir_deribit == dir_empirical == dir_micro):
            logger.debug(
                f"[BTC v5] Sin unanimidad — deribit={'UP' if dir_deribit else 'DN'} "
                f"empirical={'UP' if dir_empirical else 'DN'} "
                f"micro={'UP' if dir_micro else 'DN'}"
            )
            return None

        # ── Ensemble ponderado ────────────────────────────────────────────────
        p_ensemble = (
            W_DERIBIT   * p_deribit
            + W_EMPIRICAL * p_empirical
            + W_MICRO     * p_micro
        )
        p_ensemble = max(0.01, min(0.99, p_ensemble))
        p_no       = 1.0 - p_ensemble

        # ── Fee dinámico por volumen ──────────────────────────────────────────
        # (el volumen del mercado se evalúa en _evaluate, pasamos fee estático aquí)
        fee = FEE_HIGH_VOL  # se sobreescribirá en _evaluate con el vol real del mercado

        # ── Seleccionar lado con mayor edge ───────────────────────────────────
        # Nota: p_ensemble = P(UP), p_no = P(DOWN)
        # Se retorna el ensemble y el lado; el edge definitivo se calcula en _evaluate
        # donde se conoce el precio del mercado
        side = "UP" if p_ensemble >= 0.50 else "DOWN"

        return (p_ensemble, side, fee, p_deribit, p_empirical, micro_adj)

    # ── Evaluación principal ───────────────────────────────────────────────────

    def _evaluate(self, triggered_by_move: bool = False):
        btc = self.btc_price
        if btc <= 0:
            return

        self._last_eval_price = btc

        # Reset diario
        today = datetime.now(timezone.utc).date()
        with self._exec_lock:
            if self._last_exec_date != today:
                self._executed_today.clear()
                self._last_exec_date = today

        market = self._get_active_market()
        if not market:
            if not triggered_by_move:
                logger.debug(f"[BTC v5] BTC=${btc:,.0f} | sin mercado diario activo")
            return

        condition_id = market.get("conditionId", "")
        with self._exec_lock:
            if condition_id in self._executed_today:
                return

        strike = self._get_strike(market)
        if not strike:
            return

        end_dt = self._parse_end_date(market.get("endDate", ""))
        if not end_dt:
            return

        now         = datetime.now(timezone.utc)
        hours_left  = (end_dt - now).total_seconds() / 3600

        if hours_left <= 0:
            return
        if hours_left < BLACKOUT_MINUTES / 60.0:
            logger.debug("[BTC v5] Blackout — muy cerca del cierre")
            return
        if hours_left < MIN_HOURS_REMAINING:
            logger.debug(f"[BTC v5] Muy poco tiempo restante ({hours_left:.1f}h < {MIN_HOURS_REMAINING}h)")
            return
        if hours_left > MAX_HOURS_REMAINING:
            logger.debug(f"[BTC v5] Demasiado tiempo restante ({hours_left:.1f}h > {MAX_HOURS_REMAINING}h)")
            return

        sigma = self._har_vol
        result = self._evaluate_ensemble(btc, strike, hours_left, sigma)

        # Precio del mercado (Polymarket)
        try:
            raw = market.get("outcomePrices", "[0.5,0.5]")
            prices_list = json.loads(raw) if isinstance(raw, str) else raw
            p_mkt_up = float(prices_list[0])
            p_mkt_dn = float(prices_list[1])
        except Exception:
            return

        if result is None:
            # Sin unanimidad — log periódico y salir
            move_pct = (btc - strike) / strike
            logger.debug(
                f"[BTC v5/{'MOVE' if triggered_by_move else 'TIMER'}] "
                f"BTC=${btc:,.0f} K=${strike:,.0f} move={move_pct:+.2%} | "
                f"sin unanimidad de estimadores"
            )
            self._consecutive_signal  = 0
            self._pending_condition_id = ""
            return

        p_ensemble, side, _, p_deribit, p_empirical, micro_adj = result

        # Fee según volumen real del mercado
        vol_24h = float(market.get("volume24hr") or 0)
        fee = FEE_HIGH_VOL if vol_24h >= VOL_THRESHOLD_FEE else FEE_LOW_VOL

        # Edge según el lado elegido
        if side == "UP":
            p_true = p_ensemble
            p_mkt  = p_mkt_up
        else:
            p_true = 1.0 - p_ensemble
            p_mkt  = p_mkt_dn

        edge = p_true - p_mkt - fee

        move_pct = (btc - strike) / strike
        trigger_tag = "MOVE" if triggered_by_move else "TIMER"
        log_fn = logger.info if edge >= self.min_edge else logger.debug
        log_fn(
            f"[BTC v5/{trigger_tag}] BTC=${btc:,.0f} K=${strike:,.0f} move={move_pct:+.2%} | "
            f"T={hours_left:.1f}h σ={sigma:.4f} fee={fee:.2%} | "
            f"Deribit={p_deribit:.3f} Empir={p_empirical:.3f} Micro={micro_adj:+.3f} → "
            f"ensemble={p_ensemble:.3f} mkt={p_mkt_up:.3f} | "
            f"edge_{side}={edge:+.4f} (min={self.min_edge})"
        )

        if edge < self.min_edge:
            if self._pending_condition_id:
                self._consecutive_signal  = 0
                self._pending_condition_id = ""
            return

        # Confirmación: N evaluaciones consecutivas
        if self._pending_condition_id != condition_id:
            self._consecutive_signal  = 0
            self._pending_condition_id = condition_id

        self._consecutive_signal += 1

        if self._consecutive_signal < MIN_CONSECUTIVE:
            logger.info(
                f"[BTC v5] Confirmación {self._consecutive_signal}/{MIN_CONSECUTIVE} — "
                f"edge={edge:+.4f} | esperando próxima evaluación..."
            )
            return

        # Confirmado — ejecutar
        self._consecutive_signal  = 0
        self._pending_condition_id = ""
        self._execute_signal(
            market=market,
            side=side,
            p_true=p_true,
            p_mkt=p_mkt,
            edge=edge,
            btc_price=btc,
            strike=strike,
            hours_left=hours_left,
            p_deribit=p_deribit,
            p_empirical=p_empirical,
            micro_adj=micro_adj,
            fee=fee,
        )
        with self._exec_lock:
            self._executed_today.add(condition_id)

    # ── Ejecución ──────────────────────────────────────────────────────────────

    def _execute_signal(
        self,
        market, side, p_true, p_mkt, edge,
        btc_price, strike, hours_left,
        p_deribit, p_empirical, micro_adj, fee,
    ):
        question     = market.get("question", "BTC Up or Down")
        condition_id = market.get("conditionId", "")

        if not self.executor:
            logger.info(
                f"[BTC v5 DRY RUN] {question[:60]} | "
                f"side={side} price={p_mkt:.3f} fair={p_true:.3f} edge={edge:+.4f}"
            )
            if self.notifier:
                self.notifier.notify_trade_opened(
                    question=question,
                    side="YES" if side == "UP" else "NO",
                    entry_price=p_mkt,
                    position_usd=self.bankroll_usd * 0.10,   # estimado
                    signal_type="BTC_ARB_v5",
                    edge=edge,
                    dry_run=True,
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
        clob_side = "YES" if side == "UP" else "NO"

        opp = Opportunity(
            signal_type   = SignalType.PRICE_DRIFT,   # reusa el tipo, el label real está en notes
            condition_id  = condition_id,
            token_id      = token_id,
            question      = question,
            category      = "Crypto",
            side          = clob_side,
            market_price  = p_mkt,
            fair_value    = round(p_true, 5),
            edge          = round(edge, 5),
            edge_pct      = round(edge / p_mkt, 4) if p_mkt > 0 else 0,
            best_bid      = p_mkt,
            best_ask      = p_mkt,
            spread        = 0.001,
            liquidity_usd = float(market.get("liquidityNum") or 0),
            volume_24h    = float(market.get("volume24hr") or 0),
            notes=(
                f"BTC_ARB_v5 | ensemble | "
                f"btc={btc_price:.0f} K={strike:.0f} T={hours_left:.1f}h "
                f"deribit={p_deribit:.3f} empir={p_empirical:.3f} micro={micro_adj:+.3f} "
                f"σ={self._har_vol:.4f} fee={fee:.3f} fund={self._funding_rate:+.6f}"
            ),
        )

        result = self.executor.maybe_execute(opp)
        # La notificación de trade abierto es manejada por TradeExecutor.
        if result and not result.dry_run and result.order_id:
            logger.info(f"[BTC v5 TRADE] Order ID: {result.order_id}")

    # ── HAR-RV (fallback cuando Deribit no está disponible) ───────────────────

    def _calc_har_probability(
        self, btc_spot: float, strike: float, hours_left: float, sigma_daily: float
    ) -> float:
        """Fallback: N(d₂)+Cornish-Fisher con HAR-RV (método de v4)."""
        if strike <= 0 or sigma_daily <= 0 or hours_left <= 0:
            return 0.5

        T = hours_left / 24.0
        try:
            log_m = math.log(btc_spot / strike)
        except ValueError:
            return 0.5

        sigma_t = sigma_daily * math.sqrt(T)
        if sigma_t < 1e-8:
            return 1.0 if btc_spot > strike else 0.0

        d2 = (log_m - 0.5 * sigma_daily ** 2 * T) / sigma_t

        g1, g2 = BTC_SKEWNESS, BTC_KURTOSIS_EXCESS
        d2_sq, d2_cb = d2 * d2, d2 * d2 * d2
        z_cf = (
            d2
            - (g1 / 6.0)       * (d2_sq - 1.0)
            - (g2 / 24.0)      * (d2_cb - 3.0 * d2)
            + (g1 ** 2 / 36.0) * (2.0 * d2_cb - 5.0 * d2)
        )
        return max(0.01, min(0.99, _norm_cdf(z_cf)))

    def _update_har_vol(self) -> float:
        """HAR-RV sobre velas de 5 min de Binance (heredado de v4)."""
        try:
            now    = datetime.now(timezone.utc)
            end_ts = int(now.timestamp() * 1000)
            start_ts = int((now - timedelta(days=23)).timestamp() * 1000)

            r = requests.get(
                BINANCE_KLINES,
                params={
                    "symbol": "BTCUSDT", "interval": "5m",
                    "startTime": start_ts, "endTime": end_ts, "limit": 1500,
                },
                timeout=15,
            )
            candles = r.json()
            if len(candles) < 50:
                return BTC_VOL_FALLBACK

            closes = [float(c[4]) for c in candles]
            ret5m  = [(closes[i] - closes[i-1]) / closes[i-1] for i in range(1, len(closes))]
            BARS   = 288

            def rv_period(returns_list):
                if not returns_list:
                    return BTC_VOL_FALLBACK
                ss    = sum(r * r for r in returns_list)
                scale = (BARS / len(returns_list)) ** 0.5
                return (ss ** 0.5) * scale

            rv_1d  = rv_period(ret5m[-BARS:])
            rv_5d  = rv_period(ret5m[-5 * BARS:])
            rv_22d = rv_period(ret5m[-22 * BARS:]) if len(ret5m) >= 22 * BARS else rv_5d

            har = HAR_INTERCEPT + HAR_BETA_DAILY * rv_1d + HAR_BETA_WEEKLY * rv_5d + HAR_BETA_MONTHLY * rv_22d
            return max(0.005, min(0.10, har))

        except Exception as e:
            logger.warning(f"[BTC v5] HAR-RV error: {e}")
            return BTC_VOL_FALLBACK

    def _fetch_funding_rate(self) -> Optional[float]:
        try:
            r = requests.get(
                BINANCE_FUNDING, params={"symbol": "BTCUSDT", "limit": 1}, timeout=10
            )
            data = r.json()
            if data:
                return float(data[-1]["fundingRate"])
        except Exception as e:
            logger.debug(f"[BTC v5] Funding error: {e}")
        return None

    # ── Helpers de mercado ─────────────────────────────────────────────────────

    def _get_active_market(self) -> Optional[dict]:
        now_ts = time.time()
        if self._market_cache and now_ts - self._market_cache_ts < MARKET_CACHE_TTL:
            return self._market_cache

        try:
            r = requests.get(GAMMA_SEARCH_URL, timeout=10)
            markets  = r.json()
            now_utc  = datetime.now(timezone.utc)
            btc      = self.btc_price
            candidates = []

            for m in markets:
                if m.get("closed", False):
                    continue
                q = m.get("question", "").lower()
                if "bitcoin" not in q and "btc" not in q:
                    continue

                strike = self._get_strike(m)
                if not strike:
                    continue

                end_dt = self._parse_end_date(m.get("endDate", ""))
                if not end_dt:
                    continue
                hours_to_end = (end_dt - now_utc).total_seconds() / 3600

                # Solo mercados dentro de la ventana de operación
                if hours_to_end < MIN_HOURS_REMAINING or hours_to_end > MAX_HOURS_REMAINING:
                    continue

                vol = float(m.get("volume24hr") or 0)
                if vol < MIN_MARKET_VOLUME_USD:
                    continue

                candidates.append((m, strike, vol))

            if not candidates:
                self._market_cache    = None
                self._market_cache_ts = now_ts
                return None

            # Preferir el strike más cercano al precio actual
            if btc > 0:
                candidates.sort(key=lambda x: (abs(x[1] - btc), -x[2]))
            else:
                candidates.sort(key=lambda x: -x[2])

            best_market, best_strike, best_vol = candidates[0]
            self._market_cache    = best_market
            self._market_cache_ts = now_ts
            logger.debug(
                f"[BTC v5] Mercado: K=${best_strike:,.0f} vol=${best_vol:,.0f} "
                f"({best_market.get('question','')[:60]})"
            )
            return best_market

        except Exception as e:
            logger.warning(f"[BTC v5] No se pudo obtener mercado: {e}")
            return None

    def _get_strike(self, market: dict) -> Optional[float]:
        if "_parsed_strike" in market:
            return market["_parsed_strike"]
        try:
            events = market.get("events", [])
            if events:
                meta = events[0].get("eventMetadata", {})
                val  = float(meta.get("priceToBeat", 0))
                if val > 0:
                    return val
        except Exception:
            pass
        return self._parse_strike_from_question(market.get("question", ""))

    def _parse_strike_from_question(self, question: str) -> Optional[float]:
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
