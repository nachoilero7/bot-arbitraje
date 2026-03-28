"""
BTC Short-Window Monitor — Oracle lag en mercados "Bitcoin Up or Down" de 5-30 min.

Dos tipos de mercados cubiertos:
  A) Manuales (hourly): "Bitcoin Up or Down - March 22, 1:15PM-1:30PM ET"
     - Ventanas de 15-30 minutos, creados manualmente por Polymarket
     - Aparecen en el endpoint general de Gamma API

  B) Rolling 5-min: slug = "btc-updown-5m-{unix_timestamp}"
     - Ventanas continuas de exactamente 5 minutos (cada multiplo de 300 segundos)
     - NO aparecen en el endpoint general — se consultan por slug calculado
     - Resueltos automaticamente por Chainlink BTC/USD Data Stream

Estrategia de oracle lag:
  - El oraculo Chainlink actualiza cada 10-30 segundos (o en movimientos de ±0.5%)
  - En los ultimos ~30 segundos de una ventana, el precio de Binance (WebSocket
    en tiempo real) precede al Chainlink por 5-30 segundos
  - Si BTC se movio claramente desde priceToBeat y el mercado no lo refleja → edge

Mecanica:
  - priceToBeat: fijado al INICIO de cada ventana (precio Chainlink en ese momento)
  - Resolution: finalPrice (Chainlink al cierre) >= priceToBeat → UP gana
  - sigma_remaining: muy pequeno en los ultimos minutos → z grande → P muy alta
  - Entrada: en los ultimos ENTRY_WINDOW_SECS (120s) con MIN_SECS_REMAINING (5s)

Evaluacion: cada 5 segundos (vs 30s del monitor diario).
"""
import json
import threading
import time
from datetime import datetime, timezone
from typing import Optional, Callable

import requests
from scipy.stats import t as student_t

from src.utils.logger import get_logger

logger = get_logger(__name__)

GAMMA_SERIES_URL  = (
    "https://gamma-api.polymarket.com/markets"
    "?active=true&closed=false&limit=100&order=volume24hr&ascending=false"
)
GAMMA_SLUG_URL    = "https://gamma-api.polymarket.com/markets"

# Los mercados de 5-min reales usan el slug btc-updown-5m-{unix_ts}
# El timestamp es el inicio de la ventana (UTC, multiplo de 300)
SLUG_PREFIX_5MIN  = "btc-updown-5m-"

# Cuantas ventanas hacia adelante buscar (actual + proximas 2)
LOOKAHEAD_WINDOWS = 3

# Solo actuar en los ultimos N segundos de cada ventana
# RAZON: el oracle lag de Chainlink es de 5-30 seg. Entrar a 30 seg del cierre maximiza
# la ventana util de lag. Entrar a 120 seg es demasiado temprano: el oracle ya actualizo.
ENTRY_WINDOW_SECS = 25        # solo los ultimos 25 segundos
MIN_SECS_REMAINING = 8        # no entrar en los ultimos 8 segundos

# Cache de mercados: 30 segundos
MARKET_CACHE_TTL = 30

# Vol empirica de BTC en ventana de 5 minutos.
# La vol diaria escalada con sqrt(t) da ~0.15% por ventana de 5min.
# La vol REALIZADA en 5min es empiricamente ~0.30% (2x el sqrt scaling),
# por el efecto de microestructura y alta frecuencia de noticias intradiarias.
VOL_5MIN_EMPIRICAL = 0.003  # 0.30% por ventana de 5 minutos

# Movimiento minimo REAL entre Binance y Chainlink para considerar una señal.
# Esto mide el GAP ACTUAL entre spot y oracle, no el movimiento total de la ventana.
# < 0.20% no es suficiente para superar el costo de ejecucion y el spread.
MIN_ABS_MOVE_PCT = 0.002    # 0.20% minimo de gap Binance vs Chainlink

# Minimo lag del oraculo Chainlink para que el gap sea "explotable".
# Si el oraculo actualizo hace < 8s, puede actualizar de nuevo antes del cierre.
MIN_CHAINLINK_LAG_SECS = 8  # al menos 8s sin actualizacion del oracle

# Minimo edge para ejecutar.
MIN_EDGE_SHORT = 0.20        # 20% edge minimo

# Evaluation loop: 5 segundos
EVAL_INTERVAL_SHORT = 5

# Grados de libertad t-distribution (fat tails de BTC)
BTC_TDIST_DF = 4


class BtcShortWindowMonitor:
    """
    Monitor de oracle lag para mercados BTC de ventana corta (5-30 min).
    Requiere un BtcArbMonitor activo para compartir precio BTC en tiempo real y vol.
    """

    def __init__(
        self,
        btc_price_getter: Callable[[], float],
        vol_getter: Callable[[], float],
        executor=None,
        notifier=None,
        pnl_tracker=None,
        min_edge: float = MIN_EDGE_SHORT,
        dry_run: bool = True,
        bankroll_usd: float = 100.0,
        chainlink_getter: Callable[[], float] = None,
        chainlink_lag_getter: Callable[[], float] = None,
    ):
        self.btc_price_getter    = btc_price_getter
        self.vol_getter          = vol_getter
        self.executor            = executor
        self.notifier            = notifier
        self.pnl_tracker         = pnl_tracker
        self.min_edge            = min_edge
        self.dry_run             = dry_run
        self.bankroll_usd        = bankroll_usd
        self.chainlink_getter    = chainlink_getter     # precio actual del oraculo Chainlink
        self.chainlink_lag_getter = chainlink_lag_getter  # segundos desde la ultima actualizacion

        self._running         = False
        self._market_cache: list = []
        self._market_cache_ts: float = 0.0
        self._executed: set   = set()  # condition_ids ejecutados
        self._last_reset_date = None
        self._last_market_count: int = -1  # para evitar spam en log

    # ── Public ─────────────────────────────────────────────────────────────────

    def start(self):
        self._running = True
        threading.Thread(target=self._eval_loop, daemon=True, name="btc-5min").start()
        logger.info("[BTC 5MIN] Monitor de ventana corta iniciado")

    def stop(self):
        self._running = False

    # ── Evaluation loop ────────────────────────────────────────────────────────

    def _eval_loop(self):
        # Esperar hasta tener precio BTC
        for _ in range(30):
            if self.btc_price_getter() > 0:
                break
            time.sleep(1)

        while self._running:
            time.sleep(EVAL_INTERVAL_SHORT)
            if not self._running:
                break
            try:
                self._evaluate()
            except Exception as e:
                logger.error(f"[BTC 5MIN] Eval error: {e}")

    def _evaluate(self):
        btc = self.btc_price_getter()
        if btc <= 0:
            return

        # Reset ejecuciones diarias
        today = datetime.now(timezone.utc).date()
        if self._last_reset_date != today:
            self._executed.clear()
            self._last_reset_date = today

        markets = self._get_short_window_markets()
        if not markets:
            return

        now = datetime.now(timezone.utc)

        for market in markets:
            try:
                condition_id = market.get("conditionId", "")
                if condition_id in self._executed:
                    continue

                end_dt = self._parse_end_date(market.get("endDate", ""))
                if not end_dt:
                    continue

                secs_remaining = (end_dt - now).total_seconds()

                # Solo actuar en la ventana de entrada
                if secs_remaining > ENTRY_WINDOW_SECS or secs_remaining < MIN_SECS_REMAINING:
                    continue

                price_to_beat = self._get_price_to_beat(market)
                if not price_to_beat:
                    continue

                # ── Oracle lag detection ──────────────────────────────────────
                # MODO CHAINLINK (preferido): usar el gap REAL entre Binance spot
                # y el precio actual del oraculo Chainlink en Polygon.
                # Solo hay edge real si Binance ya se movio pero Chainlink NO actualizo aun.
                #
                # MODO FALLBACK: si no tenemos Chainlink, comparar BTC vs priceToBeat
                # (precio al inicio de la ventana). Menos preciso: puede generar señales
                # para movimientos que ya fueron capturados por el oraculo.
                chainlink = self.chainlink_getter() if self.chainlink_getter else 0.0
                chainlink_lag = self.chainlink_lag_getter() if self.chainlink_lag_getter else 0.0

                if chainlink > 0:
                    # Gap real: Binance vs Chainlink actual
                    pct_move = (btc - chainlink) / chainlink
                    # Requerir lag minimo: si el oracle actualizo hace <8s puede volver a
                    # actualizar antes del cierre y anular la ventaja
                    if chainlink_lag < MIN_CHAINLINK_LAG_SECS:
                        continue
                    ref_label = f"chain={chainlink:,.0f} lag={chainlink_lag:.1f}s"
                else:
                    # Fallback: comparar vs inicio de ventana (menos preciso)
                    pct_move = (btc - price_to_beat) / price_to_beat
                    ref_label = f"ptb={price_to_beat:,.0f} [FALLBACK]"

                # Filtrar gaps insuficientes
                if abs(pct_move) < MIN_ABS_MOVE_PCT:
                    continue

                # sigma para el tiempo restante (vol empirica de 5min escalada)
                sigma_remaining = VOL_5MIN_EMPIRICAL * (secs_remaining / 300) ** 0.5

                z = pct_move / sigma_remaining
                p_up_true = float(student_t.cdf(z, df=BTC_TDIST_DF))

                try:
                    raw_prices = market.get("outcomePrices", "[0.5,0.5]")
                    prices   = json.loads(raw_prices) if isinstance(raw_prices, str) else raw_prices
                    p_up_mkt = float(prices[0])
                    p_dn_mkt = float(prices[1])
                except Exception:
                    continue

                edge_up = p_up_true - p_up_mkt
                edge_dn = (1 - p_up_true) - p_dn_mkt

                best_edge = edge_up if abs(edge_up) >= abs(edge_dn) else edge_dn
                side      = "UP"   if abs(edge_up) >= abs(edge_dn) else "DOWN"
                p_true    = p_up_true if side == "UP" else (1 - p_up_true)
                p_mkt     = p_up_mkt  if side == "UP" else p_dn_mkt

                question = market.get("question", "")
                logger.info(
                    f"[BTC 5MIN] {question[:50]} | "
                    f"BTC={btc:,.0f} {ref_label} gap={pct_move:+.3%} | "
                    f"{secs_remaining:.0f}s left | sig={sigma_remaining:.4%} z={z:+.2f} | "
                    f"P(UP)={p_up_true:.3f} mkt={p_up_mkt:.3f} edge_{side}={best_edge:+.4f}"
                )

                if best_edge < self.min_edge:
                    continue

                self._execute(
                    market=market, side=side, p_true=p_true, p_mkt=p_mkt,
                    edge=best_edge, btc_price=btc, price_to_beat=price_to_beat,
                    secs_remaining=secs_remaining,
                )
                self._executed.add(condition_id)

            except Exception as e:
                logger.debug(f"[BTC 5MIN] Skip {market.get('conditionId','?')[:12]}: {e}")

    # ── Execute ────────────────────────────────────────────────────────────────

    def _execute(self, market, side, p_true, p_mkt, edge, btc_price, price_to_beat, secs_remaining):
        question     = market.get("question", "")
        condition_id = market.get("conditionId", "")

        if not self.executor:
            mode = "DRY RUN"
            logger.info(
                f"[BTC 5MIN {mode}] {question} | side={side} "
                f"price={p_mkt:.3f} fair={p_true:.3f} edge={edge:.4f} | {secs_remaining:.0f}s left"
            )
            if self.notifier:
                self.notifier._send(
                    f"[BTC 5MIN] {question}\n"
                    f"Side: {side} @ {p_mkt:.3f} | fair={p_true:.3f} | edge={edge:+.4f}\n"
                    f"BTC: ${btc_price:,.0f} vs ref ${price_to_beat:,.0f} | {secs_remaining:.0f}s left"
                )
            if self.pnl_tracker:
                size_usd = min(self.bankroll_usd * 0.05, 10.0)  # posicion mas chica: mas riesgo
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

        token_id = token_ids[0] if side == "UP"  and len(token_ids) > 0 else \
                   token_ids[1] if side == "DOWN" and len(token_ids) > 1 else ""

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
            notes         = f"BTC_5MIN oracle_lag btc={btc_price:.0f} ptb={price_to_beat:.0f} {secs_remaining:.0f}s",
        )

        result = self.executor.maybe_execute(opp)
        if result:
            mode = "DRY RUN" if result.dry_run else "TRADE"
            msg = (
                f"[BTC 5MIN {mode}] {question}\n"
                f"Side: {side} @ {result.price:.3f} | ${result.position_usd:.2f} | edge={edge:.4f}\n"
                f"BTC: ${btc_price:,.0f} vs ref ${price_to_beat:,.0f} | {secs_remaining:.0f}s left"
            )
            if not result.dry_run:
                msg += f"\nOrder ID: {result.order_id}"
            if self.notifier:
                self.notifier._send(msg)
            # Solo registrar si el trade fue realmente ejecutado (o es dry_run)
            # Un TradeResult con success=False significa que la API rechazó la orden —
            # no tenemos posición abierta, no hay P&L que trackear.
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

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _get_short_window_markets(self) -> list[dict]:
        """
        Obtiene mercados BTC de ventana corta activos. Cache 30s.
        Combina dos fuentes:
          A) Mercados manuales de ventana corta ("Bitcoin Up or Down -") via endpoint general
          B) Mercados de 5-min rolling via slug calculado (btc-updown-5m-{ts})
        """
        now = time.time()
        if self._market_cache and now - self._market_cache_ts < MARKET_CACHE_TTL:
            return self._market_cache

        markets = []
        seen_ids: set = set()

        # Fuente A: mercados manuales de ventana corta via endpoint general
        try:
            r = requests.get(GAMMA_SERIES_URL, timeout=10)
            for m in r.json():
                q = m.get("question", "")
                if "Bitcoin Up or Down -" in q and not m.get("closed", False):
                    cid = m.get("conditionId", "")
                    if cid not in seen_ids:
                        markets.append(m)
                        seen_ids.add(cid)
        except Exception as e:
            logger.debug(f"[BTC 5MIN] Error fetching manual markets: {e}")

        # Fuente B: mercados rolling de 5-min via slug calculado
        # El timestamp del slug es el inicio de la ventana (UTC, multiplo de 300)
        window_base = (int(now) // 300) * 300
        for i in range(LOOKAHEAD_WINDOWS):
            ts = window_base + (i * 300)
            slug = f"{SLUG_PREFIX_5MIN}{ts}"
            try:
                r = requests.get(GAMMA_SLUG_URL, params={"slug": slug}, timeout=5)
                data = r.json()
                candidates = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
                for m in candidates:
                    if not m or m.get("closed", False):
                        continue
                    cid = m.get("conditionId", "")
                    if cid and cid not in seen_ids:
                        markets.append(m)
                        seen_ids.add(cid)
                        logger.debug(f"[BTC 5MIN] Mercado rolling encontrado: {slug}")
            except Exception as e:
                logger.debug(f"[BTC 5MIN] Slug {slug} no encontrado: {e}")

        self._market_cache    = markets
        self._market_cache_ts = now
        count = len(markets)
        if count != self._last_market_count:
            if count > 0:
                logger.info(f"[BTC 5MIN] {count} mercados activos")
            else:
                logger.info("[BTC 5MIN] Sin mercados activos")
            self._last_market_count = count
        return markets

    def _get_price_to_beat(self, market: dict) -> Optional[float]:
        """
        Obtiene priceToBeat del mercado.
        - Mercados manuales: disponible en eventMetadata.priceToBeat
        - Mercados rolling 5-min: NO en API → aproximar con Binance 1m open en eventStartTime
        """
        # Intento 1: eventMetadata (mercados manuales)
        try:
            events = market.get("events", [])
            if events:
                meta = events[0].get("eventMetadata", {})
                ptb = float(meta.get("priceToBeat", 0))
                if ptb > 0:
                    return ptb
        except Exception:
            pass

        # Intento 2: Binance 1m open en eventStartTime (mercados rolling 5-min)
        event_start = market.get("eventStartTime")
        if event_start:
            return self._fetch_binance_price_at(event_start)

        return None

    def _fetch_binance_price_at(self, iso_timestamp: str) -> Optional[float]:
        """
        Obtiene el precio de apertura del candle de 1m de Binance en el timestamp dado.
        Usado como aproximacion del Chainlink priceToBeat para mercados rolling.
        Cache interno: no volver a fetchear el mismo timestamp.
        """
        if not hasattr(self, "_ptb_cache"):
            self._ptb_cache: dict = {}

        if iso_timestamp in self._ptb_cache:
            return self._ptb_cache[iso_timestamp]

        try:
            from datetime import datetime, timezone
            dt = datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00"))
            ts_ms = int(dt.timestamp() * 1000)
            r = requests.get(
                "https://api.binance.com/api/v3/klines",
                params={"symbol": "BTCUSDT", "interval": "1m", "startTime": ts_ms, "limit": 1},
                timeout=5,
            )
            candle = r.json()
            if candle and isinstance(candle, list):
                price = float(candle[0][1])  # open price del candle de 1m
                self._ptb_cache[iso_timestamp] = price
                return price
        except Exception as e:
            logger.debug(f"[BTC 5MIN] No se pudo obtener Binance price @ {iso_timestamp}: {e}")
        return None

    def _parse_end_date(self, s: str) -> Optional[datetime]:
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
        except Exception:
            return None
