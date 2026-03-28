"""
BTC Arb Monitor — Monitor en tiempo real del mercado "Bitcoin Up or Down" diario.

Mejoras v2:
  #1 Precio real de Polymarket: usa outcomePrices de Gamma API (cache 30s)
  #2 Event-driven: dispara evaluacion inmediata cuando BTC se mueve >0.3%
  #3 Volatilidad dinamica: vol realizada de los ultimos 7 dias

Mejoras v3:
  #4 t-Student: reemplaza normal por t-distribution (df=4) para fat tails de crypto
  #5 Funding rate: ajusta P(UP) segun posicionamiento del mercado de futuros perp
  #6 Entry confirmation: requiere 2 evaluaciones consecutivas con edge antes de ejecutar
  #7 P&L tracking: registra cada trade y monitorea la resolucion

Estrategia:
  Polymarket publica cada dia un mercado binario "BTC Up or Down on <fecha>?"
  Resolucion: precio de cierre 1m de BTC/USDT en Binance al mediodia ET de hoy vs ayer.
  Edge cuando el movimiento acumulado es claro pero Polymarket todavia no lo refleja.

Modelo de probabilidad (v3):
  z = pct_move / sigma_remaining
  p_up_base = t.cdf(z, df=4)                          # fat tails
  funding_adj = funding_rate * FUNDING_BIAS_SCALE      # posicionamiento de mercado
  p_up_true = clamp(p_up_base - funding_adj, 0.01, 0.99)
"""
import asyncio
import json
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests
import websockets
from scipy.stats import t as student_t

from src.utils.logger import get_logger

logger = get_logger(__name__)

BINANCE_WS_URL   = "wss://stream.binance.com:9443/ws/btcusdt@ticker"
BINANCE_KLINES   = "https://api.binance.com/api/v3/klines"
BINANCE_FUNDING  = "https://fapi.binance.com/fapi/v1/fundingRate"
GAMMA_SERIES_URL = "https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=50&order=volume24hr&ascending=false"

# Chainlink BTC/USD Data Feed en Polygon (actualiza cada 10-30s o si BTC se mueve ±0.5%)
# NOTA: 0xF9680D99...  = ETH/USD (INCORRECTO para BTC)
#       0xc907E116...  = BTC/USD verificado ($70,205 vs Binance $70,344 el 23/03/26)
CHAINLINK_BTC_POLYGON  = "0xc907E116054Ad103354f2D350FD2514433D57F6f"
CHAINLINK_SELECTOR     = "0xfeaf968c"  # latestRoundData()
CHAINLINK_POLL_SECS    = 5             # pollear cada 5s para detectar actualizaciones
# Rango valido de precio BTC (sanidad): si el oraculo devuelve fuera de este rango, ignorar
CHAINLINK_BTC_MIN = 10_000
CHAINLINK_BTC_MAX = 200_000

# Volatilidad fallback si no se puede calcular la dinamica
BTC_DAILY_VOL_FALLBACK = 0.025

# Minimo edge para ejecutar
MIN_EDGE_BTC = 0.08

# No operar en los ultimos minutos antes del cierre
BLACKOUT_MINUTES_BEFORE_CLOSE = 10

# No operar en las primeras horas del dia
MIN_HOURS_ELAPSED = 2.0

# Intervalo de evaluacion periodica (fallback cuando BTC no se mueve)
EVAL_INTERVAL = 30

# #2 — Umbral de movimiento para disparo inmediato (0.3%)
MOVE_TRIGGER_PCT = 0.003

# #1 — Cache del mercado: 30s
MARKET_CACHE_TTL = 30

# #3 — Cache de volatilidad: 1 hora
VOL_CACHE_TTL = 3600

# #4 — Grados de libertad de la t-distribution para BTC (fat tails crypto)
BTC_TDIST_DF = 4

# #5 — Funding rate
FUNDING_CACHE_TTL = 3600   # actualizar cada hora
FUNDING_BIAS_SCALE = 50    # escala: funding 0.001 → ajuste de 0.05 en probabilidad

# #6 — Confirmacion de entrada: requiere N evaluaciones consecutivas con edge
MIN_CONSECUTIVE_SIGNALS = 2


class BtcArbMonitor:
    """
    Monitor de arbitraje para el mercado diario 'Bitcoin Up or Down'.
    Corre en threads separados junto al scanner principal.
    """

    def __init__(
        self,
        executor=None,
        notifier=None,
        pnl_tracker=None,
        min_edge: float = MIN_EDGE_BTC,
        dry_run: bool = True,
        bankroll_usd: float = 100.0,
        proxy: str = None,
        alchemy_api_key: str = None,
    ):
        self.executor         = executor
        self.notifier         = notifier
        self.pnl_tracker      = pnl_tracker
        self.min_edge         = min_edge
        self.dry_run          = dry_run
        self.bankroll_usd     = bankroll_usd
        self.proxy            = proxy
        self.alchemy_api_key  = alchemy_api_key

        self._btc_price: float       = 0.0
        # Chainlink oracle state
        self._chainlink_price: float    = 0.0
        self._chainlink_updated_at: float = 0.0  # unix timestamp de la ultima actualizacion
        self._price_lock             = threading.Lock()
        self._running: bool          = False
        self._market_cache: dict     = {}
        self._market_cache_ts: float = 0.0
        self._executed_today: set    = set()
        self._last_exec_date         = None

        # #2 — event-driven
        self._last_eval_price: float = 0.0
        self._trigger_eval           = threading.Event()

        # #3 — vol dinamica
        self._realized_vol: float    = BTC_DAILY_VOL_FALLBACK
        self._vol_cache_ts: float    = 0.0

        # #5 — funding rate
        self._funding_rate: float    = 0.0
        self._funding_cache_ts: float = 0.0

        # #6 — confirmacion de entrada
        self._consecutive_signal: int  = 0
        self._pending_condition_id: str = ""

    # ── Public ─────────────────────────────────────────────────────────────────

    def start(self):
        self._running = True
        threading.Thread(target=self._ws_thread,          daemon=True, name="btc-ws").start()
        threading.Thread(target=self._eval_loop,          daemon=True, name="btc-eval").start()
        threading.Thread(target=self._vol_updater,        daemon=True, name="btc-vol").start()
        threading.Thread(target=self._funding_updater,    daemon=True, name="btc-funding").start()
        if self.alchemy_api_key:
            threading.Thread(target=self._chainlink_updater, daemon=True, name="btc-chainlink").start()
            logger.info("[BTC ARB] Monitor iniciado — Chainlink oracle feed activo")
        else:
            logger.info("[BTC ARB] Monitor iniciado — esperando precio BTC... (sin Chainlink: ALCHEMY_API_KEY no configurado)")

    def stop(self):
        self._running = False
        self._trigger_eval.set()

    @property
    def btc_price(self) -> float:
        with self._price_lock:
            return self._btc_price

    @property
    def chainlink_price(self) -> float:
        """Ultimo precio BTC/USD reportado por el oraculo Chainlink en Polygon."""
        return self._chainlink_price

    @property
    def chainlink_lag_secs(self) -> float:
        """Segundos desde la ultima actualizacion del oraculo Chainlink.
        Un valor alto (>10s) indica que el oraculo esta desactualizado respecto a Binance."""
        if self._chainlink_updated_at <= 0:
            return 0.0
        return time.time() - self._chainlink_updated_at

    # ── WebSocket Binance ───────────────────────────────────────────────────────

    def _ws_thread(self):
        while self._running:
            try:
                asyncio.run(self._ws_connect())
            except Exception as e:
                logger.warning(f"[BTC ARB] WebSocket error: {e} — reconectando en 5s")
                time.sleep(5)

    async def _ws_connect(self):
        async with websockets.connect(BINANCE_WS_URL, ping_interval=20) as ws:
            logger.info("[BTC ARB] WebSocket Binance conectado")
            async for raw in ws:
                if not self._running:
                    break
                try:
                    data  = json.loads(raw)
                    price = float(data.get("c", 0) or data.get("p", 0))
                    if price <= 0:
                        continue
                    with self._price_lock:
                        self._btc_price = price

                    # #2 — disparar evaluacion si el movimiento supera el umbral
                    if self._last_eval_price > 0:
                        move = abs(price - self._last_eval_price) / self._last_eval_price
                        if move >= MOVE_TRIGGER_PCT:
                            self._trigger_eval.set()
                except Exception:
                    pass

    # ── Evaluation Loop ─────────────────────────────────────────────────────────

    def _eval_loop(self):
        """
        Espera hasta que:
          a) BTC se mueve >0.3% desde la ultima evaluacion (event-driven), o
          b) pasan 30 segundos sin movimiento significativo (timer fallback)
        """
        for _ in range(30):
            if self.btc_price > 0:
                break
            time.sleep(1)

        while self._running:
            triggered = self._trigger_eval.wait(timeout=EVAL_INTERVAL)
            self._trigger_eval.clear()

            if not self._running:
                break

            try:
                self._evaluate(triggered_by_move=triggered)
            except Exception as e:
                logger.error(f"[BTC ARB] Eval error: {e}")

    # ── Chainlink Oracle ─────────────────────────────────────────────────────────

    def _chainlink_updater(self):
        """
        Thread que lee el oraculo Chainlink BTC/USD en Polygon cada 5 segundos.
        Expone chainlink_price y chainlink_lag_secs para que el short monitor pueda
        calcular el gap REAL entre Binance spot y el oraculo.
        """
        while self._running:
            try:
                price, updated_at = self._fetch_chainlink()
                # Validar que el precio es BTC (no ETH/USD u otro feed)
                if price > 0 and updated_at > 0:
                    if not (CHAINLINK_BTC_MIN < price < CHAINLINK_BTC_MAX):
                        logger.warning(
                            f"[CHAINLINK] Precio fuera de rango BTC ({price:,.0f}) — "
                            f"verificar address del feed. Ignorando."
                        )
                    else:
                        self._chainlink_price      = price
                        self._chainlink_updated_at = updated_at
                if self._chainlink_price > 0:
                    price  = self._chainlink_price
                    lag = time.time() - self._chainlink_updated_at
                    logger.debug(
                        f"[CHAINLINK] BTC={price:,.2f} | "
                        f"lag={lag:.1f}s | gap_vs_binance={(self._btc_price - price)/price:+.4%}"
                        if self._btc_price > 0 else
                        f"[CHAINLINK] BTC={price:,.2f} | lag={lag:.1f}s"
                    )
            except Exception as e:
                logger.debug(f"[CHAINLINK] Error fetching oracle: {e}")
            time.sleep(CHAINLINK_POLL_SECS)

    def _fetch_chainlink(self) -> tuple[float, float]:
        """
        Lee latestRoundData() del oraculo Chainlink BTC/USD en Polygon via Alchemy.
        Retorna (price_usd, updated_at_unix_ts).
        """
        url = f"https://polygon-mainnet.g.alchemy.com/v2/{self.alchemy_api_key}"
        payload = {
            "id": 1,
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [
                {"to": CHAINLINK_BTC_POLYGON, "data": CHAINLINK_SELECTOR},
                "latest",
            ],
        }
        r = requests.post(url, json=payload, timeout=5)
        result_hex = r.json().get("result", "0x")
        if not result_hex or result_hex == "0x":
            return 0.0, 0.0
        raw = bytes.fromhex(result_hex[2:])
        # ABI layout: roundId(32) | answer(32) | startedAt(32) | updatedAt(32) | answeredInRound(32)
        if len(raw) < 128:
            return 0.0, 0.0
        answer     = int.from_bytes(raw[32:64],  "big", signed=True)
        updated_at = int.from_bytes(raw[96:128], "big", signed=False)
        return answer / 1e8, float(updated_at)

    # ── #3 Volatilidad Dinamica ─────────────────────────────────────────────────

    def _vol_updater(self):
        """Thread que actualiza la vol realizada cada hora."""
        while self._running:
            vol = self._calc_realized_vol()
            if vol > 0:
                self._realized_vol = vol
                self._vol_cache_ts = time.time()
                logger.info(f"[BTC ARB] Vol realizada 7d: {vol:.3f} ({vol*100:.2f}%/dia)")
            time.sleep(VOL_CACHE_TTL)

    def _calc_realized_vol(self) -> float:
        """Calcula la volatilidad diaria realizada de BTC de los ultimos 7 dias."""
        try:
            now      = datetime.now(timezone.utc)
            end_ts   = int(now.timestamp() * 1000)
            start_ts = int((now - timedelta(days=8)).timestamp() * 1000)
            r = requests.get(
                BINANCE_KLINES,
                params={"symbol": "BTCUSDT", "interval": "1d", "startTime": start_ts,
                        "endTime": end_ts, "limit": 8},
                timeout=10,
            )
            candles = r.json()
            if len(candles) < 3:
                return BTC_DAILY_VOL_FALLBACK

            closes  = [float(c[4]) for c in candles]
            returns = [(closes[i] - closes[i-1]) / closes[i-1] for i in range(1, len(closes))]
            mean    = sum(returns) / len(returns)
            var     = sum((x - mean) ** 2 for x in returns) / len(returns)
            vol     = var ** 0.5
            # Clamp entre 1% y 8% para evitar valores extremos
            return max(0.01, min(0.08, vol))
        except Exception as e:
            logger.warning(f"[BTC ARB] No se pudo calcular vol dinamica: {e}")
            return BTC_DAILY_VOL_FALLBACK

    # ── #5 Funding Rate ─────────────────────────────────────────────────────────

    def _funding_updater(self):
        """Thread que actualiza el funding rate de BTC cada hora."""
        while self._running:
            rate = self._fetch_funding_rate()
            if rate is not None:
                self._funding_rate    = rate
                self._funding_cache_ts = time.time()
                direction = "long-heavy" if rate > 0 else "short-heavy"
                logger.info(
                    f"[BTC ARB] Funding rate: {rate:+.6f}/8h ({direction}) — "
                    f"ajuste P(UP): {-rate * FUNDING_BIAS_SCALE:+.3f}"
                )
            time.sleep(FUNDING_CACHE_TTL)

    def _fetch_funding_rate(self) -> Optional[float]:
        """
        Obtiene el funding rate actual del perpetuo BTCUSDT en Binance Futures.
        Positivo = longs pagan shorts (mercado sobre-comprado → presion bajista).
        Negativo = shorts pagan longs (mercado sobre-vendido → presion alcista).
        """
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
            logger.warning(f"[BTC ARB] No se pudo obtener funding rate: {e}")
        return None

    # ── Evaluate ────────────────────────────────────────────────────────────────

    def _evaluate(self, triggered_by_move: bool = False):
        btc = self.btc_price
        if btc <= 0:
            return

        self._last_eval_price = btc

        # Reset ejecuciones si cambio el dia
        today = datetime.now(timezone.utc).date()
        if self._last_exec_date != today:
            self._executed_today.clear()
            self._last_exec_date = today

        # #1 — Obtener mercado con precio real (cache 30s)
        market = self._get_active_market()
        if not market:
            if not triggered_by_move:
                logger.info(f"[BTC ARB] BTC=${btc:,.0f} | Sin mercado activo por el momento")
            return

        condition_id = market.get("conditionId", "")
        if condition_id in self._executed_today:
            return

        price_to_beat = self._get_price_to_beat(market)
        if not price_to_beat:
            logger.info(f"[BTC ARB] Sin priceToBeat aun: {market.get('question')}")
            return

        end_dt = self._parse_end_date(market.get("endDate", ""))
        if not end_dt:
            return

        now           = datetime.now(timezone.utc)
        hours_left    = (end_dt - now).total_seconds() / 3600
        hours_elapsed = 24 - hours_left

        if hours_left <= 0:
            logger.info(f"[BTC ARB] Mercado {market.get('question')} ya cerro")
            return
        if hours_left < BLACKOUT_MINUTES_BEFORE_CLOSE / 60:
            logger.debug("[BTC ARB] Blackout period — muy cerca del cierre")
            return
        if hours_elapsed < MIN_HOURS_ELAPSED:
            logger.debug(f"[BTC ARB] Muy temprano ({hours_elapsed:.1f}h transcurridas)")
            return

        # #3 — Vol realizada dinamica
        vol  = self._realized_vol
        pct_move        = (btc - price_to_beat) / price_to_beat
        sigma_remaining = vol * (hours_left / 24) ** 0.5

        # #4 — t-distribution (fat tails) en vez de normal
        if sigma_remaining > 0:
            z = pct_move / sigma_remaining
            p_up_base = float(student_t.cdf(z, df=BTC_TDIST_DF))
        else:
            p_up_base = 0.5

        # #5 — Ajuste por funding rate
        # Funding positivo → mercado sobre-comprado → presion bajista → reducir P(UP)
        funding_adj = self._funding_rate * FUNDING_BIAS_SCALE
        p_up_true = max(0.01, min(0.99, p_up_base - funding_adj))

        # #1 — Precio real del mercado desde Gamma API
        try:
            raw_prices = market.get("outcomePrices", "[0.5,0.5]")
            prices   = json.loads(raw_prices) if isinstance(raw_prices, str) else raw_prices
            p_up_mkt = float(prices[0])
            p_dn_mkt = float(prices[1])
        except Exception:
            return

        edge_up = p_up_true - p_up_mkt
        edge_dn = (1 - p_up_true) - p_dn_mkt

        best_edge = edge_up if abs(edge_up) >= abs(edge_dn) else edge_dn
        side      = "UP"  if abs(edge_up) >= abs(edge_dn) else "DOWN"
        p_true    = p_up_true if side == "UP" else (1 - p_up_true)
        p_mkt     = p_up_mkt  if side == "UP" else p_dn_mkt

        trigger_tag = "MOVE" if triggered_by_move else "TIMER"
        funding_tag = f"fund={self._funding_rate:+.5f}" if self._funding_rate != 0 else "fund=n/a"
        # Evaluaciones de movimiento siempre visibles; timers solo en DEBUG para reducir ruido
        log_fn = logger.info if triggered_by_move or best_edge >= self.min_edge else logger.debug
        log_fn(
            f"[BTC ARB/{trigger_tag}] BTC={btc:,.0f} | ref={price_to_beat:,.0f} | "
            f"move={pct_move:+.2%} | vol={vol:.3f} | sigma={sigma_remaining:.3f} | "
            f"p_base={p_up_base:.3f} {funding_tag} P(UP)={p_up_true:.3f} mkt={p_up_mkt:.3f} | "
            f"edge_{side}={best_edge:+.4f} | {hours_left:.1f}h left"
        )

        if best_edge < self.min_edge:
            # #6 — resetear confirmacion si el edge cae
            if self._pending_condition_id:
                self._consecutive_signal = 0
                self._pending_condition_id = ""
            return

        # #6 — Entry confirmation: requerir N evaluaciones consecutivas
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

        # Confirmado — ejecutar
        self._consecutive_signal  = 0
        self._pending_condition_id = ""
        self._execute_signal(
            market=market, side=side, p_true=p_true, p_mkt=p_mkt,
            edge=best_edge, btc_price=btc, price_to_beat=price_to_beat, hours_left=hours_left,
        )
        self._executed_today.add(condition_id)

    # ── Execute Signal ──────────────────────────────────────────────────────────

    def _execute_signal(self, market, side, p_true, p_mkt, edge, btc_price, price_to_beat, hours_left):
        question     = market.get("question", "BTC Up or Down")
        condition_id = market.get("conditionId", "")

        if not self.executor:
            mode = "DRY RUN"
            logger.info(
                f"[BTC ARB {mode}] {question} | side={side} "
                f"price={p_mkt:.3f} fair={p_true:.3f} edge={edge:.4f}"
            )
            if self.notifier:
                self.notifier._send(
                    f"[BTC ARB] {question}\n"
                    f"Side: {side} @ {p_mkt:.3f} | fair={p_true:.3f} | edge={edge:+.4f}\n"
                    f"BTC: ${btc_price:,.0f} vs ref ${price_to_beat:,.0f} | {hours_left:.1f}h left"
                )
            # #7 — Registrar en P&L tracker aunque no haya executor
            if self.pnl_tracker:
                size_usd = min(self.bankroll_usd * 0.1, 20.0)  # estimacion conservadora
                self.pnl_tracker.record_entry(
                    condition_id=condition_id,
                    question=question,
                    side=side,
                    entry_price=p_mkt,
                    fair_value=p_true,
                    edge=edge,
                    size_usd=size_usd,
                    dry_run=True,
                )
            return

        from src.signals.base import Opportunity, SignalType
        token_ids = []
        try:
            token_ids = json.loads(market.get("clobTokenIds", "[]"))
        except Exception:
            pass

        token_id = token_ids[0] if side == "UP"   and len(token_ids) > 0 else \
                   token_ids[1] if side == "DOWN"  and len(token_ids) > 1 else ""

        opp = Opportunity(
            signal_type   = SignalType.PARITY,
            condition_id  = condition_id,
            token_id      = token_id,
            question      = question,
            category      = "Crypto",
            side          = side,
            market_price  = p_mkt,
            fair_value    = p_true,
            edge          = edge,
            edge_pct      = edge / p_mkt if p_mkt > 0 else 0,
            best_bid      = p_mkt,
            best_ask      = p_mkt,
            spread        = 0.001,
            liquidity_usd = market.get("liquidityNum", 0),
            volume_24h    = market.get("volume24hr", 0),
            notes         = f"BTC_ARB btc={btc_price:.0f} ref={price_to_beat:.0f} vol={self._realized_vol:.3f} fund={self._funding_rate:+.5f} {hours_left:.1f}h",
        )

        result = self.executor.maybe_execute(opp)
        if result:
            mode = "DRY RUN" if result.dry_run else "TRADE"
            msg = (
                f"[BTC ARB {mode}] {question}\n"
                f"Side: {side} @ {result.price:.3f} | ${result.position_usd:.2f} | edge={edge:.4f}\n"
                f"BTC: ${btc_price:,.0f} vs ref ${price_to_beat:,.0f} | {hours_left:.1f}h left"
            )
            if not result.dry_run:
                msg += f"\nOrder ID: {result.order_id}"
            if self.notifier:
                self.notifier._send(msg)

            # #7 — Registrar en P&L tracker solo si la orden fue aceptada
            if self.pnl_tracker and (result.dry_run or result.success):
                self.pnl_tracker.record_entry(
                    condition_id=condition_id,
                    question=question,
                    side=side,
                    entry_price=result.price,
                    fair_value=p_true,
                    edge=edge,
                    size_usd=result.position_usd,
                    dry_run=result.dry_run,
                )

    # ── Helpers ─────────────────────────────────────────────────────────────────

    def _get_active_market(self) -> Optional[dict]:
        """
        Obtiene el mercado BTC activo para hoy. Cache de 30s (#1).
        Soporta tanto el formato legacy 'Bitcoin Up or Down' como el nuevo
        'Will the price of Bitcoin be above $X on <fecha>?'
        Prefiere el mercado con strike mas cercano al precio actual (maxima incertidumbre).
        """
        now = time.time()
        if self._market_cache and now - self._market_cache_ts < MARKET_CACHE_TTL:
            return self._market_cache
        try:
            today_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            r = requests.get(GAMMA_SERIES_URL, timeout=10)
            candidates = []
            now_utc = datetime.now(timezone.utc)
            for m in r.json():
                if m.get("closed", False):
                    continue
                q = m.get("question", "").lower()
                end = m.get("endDateIso", m.get("endDate", ""))[:10]
                # Formato legacy
                if "bitcoin up or down" in q:
                    end_dt = self._parse_end_date(m.get("endDate", ""))
                    if end_dt and end_dt <= now_utc:
                        continue
                    strike = self._get_price_to_beat(m)
                    candidates.append((m, strike or 0))
                    continue
                # Formato nuevo: "will the price of bitcoin be above $X on <fecha>"
                if "will the price of bitcoin be above" in q and end == today_iso:
                    end_dt = self._parse_end_date(m.get("endDate", ""))
                    if end_dt and end_dt <= now_utc:
                        continue
                    strike = self._parse_strike_from_question(m.get("question", ""))
                    if strike:
                        m["_parsed_strike"] = strike
                        candidates.append((m, strike))
            if not candidates:
                return None
            # Escoger el strike mas cercano al precio actual de BTC
            btc = self.btc_price
            if btc > 0:
                candidates.sort(key=lambda x: abs(x[1] - btc))
            best, strike = candidates[0]
            self._market_cache    = best
            self._market_cache_ts = now
            logger.debug(f"[BTC ARB] Mercado seleccionado: strike=${strike:,.0f} ({best.get('question','')[:60]})")
            return best
        except Exception as e:
            logger.warning(f"[BTC ARB] No se pudo obtener mercado activo: {e}")
        return None

    def _parse_strike_from_question(self, question: str) -> Optional[float]:
        """Extrae el strike de 'Will the price of Bitcoin be above $70,000 on...'"""
        import re
        m = re.search(r'\$([0-9,]+)', question)
        if m:
            try:
                return float(m.group(1).replace(',', ''))
            except ValueError:
                pass
        return None

    def _get_price_to_beat(self, market: dict) -> Optional[float]:
        # Strike pre-parseado (mercados nuevos "above $X")
        if "_parsed_strike" in market:
            return market["_parsed_strike"]
        # Legacy: eventMetadata.priceToBeat
        try:
            events = market.get("events", [])
            if events:
                meta = events[0].get("eventMetadata", {})
                val = float(meta.get("priceToBeat", 0))
                if val > 0:
                    return val
        except Exception:
            pass
        # Fallback: parsear del titulo
        return self._parse_strike_from_question(market.get("question", ""))

    def _parse_end_date(self, s: str) -> Optional[datetime]:
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
        except Exception:
            return None
