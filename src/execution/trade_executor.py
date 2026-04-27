"""
Trade Executor — Phase 2
Ejecuta ordenes en Polymarket via CLOB API cuando se detecta un edge suficiente.

Modos:
  dry_run=True  (default) — simula trades sin ejecutar nada real
  dry_run=False           — ejecuta trades reales (requiere CLOB_API_KEY configurado)

Seguridad:
  - MIN_EDGE_TO_TRADE: edge minimo mas alto que el de deteccion (default 6%)
  - MAX_POSITION_USD: limite por trade (default $20)
  - MAX_DAILY_LOSS_USD: frena el bot si las perdidas del dia superan este limite
  - Kelly fraccional al 25% del Kelly completo
"""
import csv
import json
import os
import threading
import time
from datetime import datetime, date
from dataclasses import dataclass

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType, MarketOrderArgs, BalanceAllowanceParams, AssetType

from src.signals.base import Opportunity
from src.sizing.kelly import calculate_kelly
from src.utils.logger import get_logger

logger = get_logger(__name__)

POLYGON_CHAIN_ID = 137
CLOB_HOST        = "https://clob.polymarket.com"

# Horizonte maximo de ejecucion por tipo de señal (en dias).
# Basado en la investigacion de prediction market edges:
#   - RESOLUTION_LAG:   el edge existe porque el resultado YA es conocido → operar solo si resuelve pronto
#   - SPREAD_CAPTURE:   market making, queres capital rotando rapido
#   - PRICE_DRIFT:      el momentum se diluye si el mercado tiene mucho tiempo por delante
#   - OVERPRICED_NO:    similar a drift, condicion de mercado transitoria
#   - CALIBRATION_BIAS: sesgo sistematico documentado en 124M trades, valido a cualquier horizonte
#   - PARITY:           arbitraje puro, pero capital inmovilizado 30 dias es ineficiente
#   - MISPRICED_CORR:   violaciones logicas pueden persistir, horizonte mas amplio OK
SIGNAL_MAX_DAYS: dict[str, int] = {
    "RESOLUTION_LAG":   3,   # resultado ya determinado, solo lag de precio
    "SPREAD_CAPTURE":   3,   # market making, rotacion rapida de capital
    "OVERPRICED_NO":    5,   # condicion transitoria de mercado
    "CALIBRATION_BIAS": 14,  # sesgo sistematico, valido a mediano plazo (SSRN 5910522)
    "PARITY":           7,   # arb puro pero preferir mercados cercanos
    "MISPRICED_CORR":   10,  # violaciones logicas / exclusion mutua — Masters-style arbs
}

# Edge mínimo por señal — override del global MIN_EDGE_TO_TRADE.
# CALIBRATION_BIAS tiene edges chicos (1-3%) pero estadísticamente significativos en 124M trades.
SIGNAL_MIN_EDGE: dict[str, float] = {
    "CALIBRATION_BIAS": 0.01,  # 1% — el paper documenta 3-5% de mispricing, fees son 2%
    "MISPRICED_CORR":   0.03,  # 3% — mutex arbs
}


@dataclass
class TradeResult:
    opportunity: Opportunity
    position_usd: float
    price: float
    size: float          # tokens comprados
    order_id: str        # "" si dry_run
    dry_run: bool
    success: bool
    error: str = ""
    executed_at: datetime = None

    def __post_init__(self):
        if self.executed_at is None:
            self.executed_at = datetime.utcnow()


class TradeExecutor:

    def __init__(
        self,
        private_key: str,
        api_key: str,
        api_secret: str,
        api_passphrase: str,
        bankroll_usd: float = 100.0,
        min_edge_to_trade: float = 0.06,   # 6% — mas conservador que el 3% de deteccion
        max_position_usd: float = 20.0,    # maximo $20 por trade
        max_daily_loss_usd: float = 30.0,  # frena si perdemos mas de $30 en el dia
        kelly_fraction: float = 0.25,
        max_days_to_resolution: int = 7,   # solo ejecutar en mercados que cierran dentro de N dias
        dry_run: bool = True,
        trades_csv: str = "data/trades.csv",
        proxy_address: str = None,         # Gnosis Safe (funder) — POLY_GNOSIS_SAFE mode
        notifier=None,                     # TelegramNotifier (opcional)
    ):
        self.bankroll_usd           = bankroll_usd
        self.min_edge_to_trade      = min_edge_to_trade
        self.max_position_usd       = max_position_usd
        self.max_daily_loss_usd     = max_daily_loss_usd
        self.kelly_fraction         = kelly_fraction
        self.max_days_to_resolution = max_days_to_resolution
        self.dry_run           = dry_run
        self.trades_csv        = trades_csv

        self._proxy_address    = proxy_address   # None = EOA, str = POLY_GNOSIS_SAFE
        self.notifier          = notifier
        self._daily_loss: float = 0.0
        self._loss_date: date   = date.today()
        self._trades_today: int = 0
        self._executed_ids: set = set()  # evitar duplicados: bloquea condition_id completo (cualquier lado)
        self._executed_questions: list = []   # [(question, end_date, side), ...] — detecta mercados correlados
        self._failed_cooldown: dict = {}   # condition_id → timestamp del último FAK rejection
        self._lock = threading.Lock()    # serializa maybe_execute entre threads
        self._state_file = "data/executor_state.json"
        self._signal_calibration: dict = {}   # signal_name → multiplier (1.0 = neutral)
        self._calibration_trades_count = 0    # trades cuando se computó por última vez

        # Cargar estado persistido del dia actual (sobrevive reinicios del bot)
        self._load_state()
        self._refresh_calibration()

        # Inicializar cliente CLOB
        creds = ApiCreds(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
        )
        # signature_type=2 (POLY_GNOSIS_SAFE): EOA firma en nombre del Gnosis Safe (funder)
        # signature_type=0 (EOA): firma directa con el EOA como portfolio
        if proxy_address:
            self.client = ClobClient(
                host=CLOB_HOST,
                key=private_key,
                chain_id=POLYGON_CHAIN_ID,
                creds=creds,
                signature_type=2,   # POLY_GNOSIS_SAFE
                funder=proxy_address,
            )
            logger.info(f"TradeExecutor: POLY_GNOSIS_SAFE mode (funder={proxy_address[:10]}...)")
        else:
            self.client = ClobClient(
                host=CLOB_HOST,
                key=private_key,
                chain_id=POLYGON_CHAIN_ID,
                creds=creds,
            )

        self._ensure_csv()
        mode = "DRY RUN" if dry_run else "LIVE"

        # Obtener balance real de la cuenta al iniciar
        real_balance = self.get_balance()
        if real_balance > 0:
            self.bankroll_usd = real_balance
            logger.info(f"TradeExecutor initialized [{mode}] balance=${real_balance:.2f} max_pos=${max_position_usd} min_edge={min_edge_to_trade:.0%}")
        else:
            logger.info(f"TradeExecutor initialized [{mode}] bankroll=${bankroll_usd} (balance no disponible) max_pos=${max_position_usd} min_edge={min_edge_to_trade:.0%}")

    # ── Public API ─────────────────────────────────────────────────────────────

    def maybe_execute(self, opportunity: Opportunity) -> TradeResult | None:
        """
        Evalua si la oportunidad califica para ejecutar un trade.
        Retorna TradeResult si se ejecuto (o simulo), None si se descarto.
        Thread-safe: serializado con lock para evitar duplicados entre threads.
        """
        with self._lock:
            return self._maybe_execute_locked(opportunity)

    def _maybe_execute_locked(self, opportunity: Opportunity) -> TradeResult | None:
        # Reset diario: solo resetea contadores diarios (loss y trades_today).
        # _executed_ids y _executed_questions NO se limpian por tiempo — solo cuando
        # la posicion resuelve (won/lost). Limpiarlos por día causa self-hedging:
        # si hoy compramos YES y mañana el reset borra el id, pasado mañana podemos
        # comprar NO en el mismo mercado.
        if date.today() != self._loss_date:
            self._daily_loss   = 0.0
            self._loss_date    = date.today()
            self._trades_today = 0
            self._failed_cooldown.clear()
            # Podar executed_ids: sacar los que ya resolvieron (won/lost) segun pnl_state
            self._prune_resolved_ids()
            self._save_state()

        # Filtros de seguridad
        if not self._passes_filters(opportunity):
            return None

        # En modo live: refrescar balance real antes de CADA trade.
        # En dry_run: NO sobreescribir bankroll_usd con el balance real — el dry_run
        # usa el BANKROLL_USD configurado para simular con el capital planeado.
        # Solo logueamos el balance real ocasionalmente para informacion.
        if not self.dry_run:
            real = self.get_balance()
            if real > 0:
                self.bankroll_usd = real
                logger.info(f"[EXECUTOR] Balance actualizado: ${real:.2f}")
        elif self._trades_today % 20 == 0:
            real = self.get_balance()
            if real > 0:
                logger.info(f"[EXECUTOR] Balance real (dry_run usa bankroll config=${self.bankroll_usd:.2f}): ${real:.2f}")

        # Si el balance real es menor que la posicion minima, no tiene sentido operar
        if not self.dry_run and self.bankroll_usd < 1.0:
            logger.warning(f"[EXECUTOR] Balance insuficiente (${self.bankroll_usd:.2f}) — sin fondos para operar.")
            self._daily_loss = self.max_daily_loss_usd  # activar kill switch permanentemente
            return None

        # Calcular tamano via Kelly
        kelly = calculate_kelly(
            p_true=opportunity.fair_value,
            p_market=opportunity.market_price,
            bankroll_usd=self.bankroll_usd,
            kelly_fraction=self.kelly_fraction,
            max_position_usd=self.max_position_usd,
        )

        if kelly.position_usd < 1.0:
            logger.debug(f"[EXECUTOR] Skip {opportunity.condition_id[:12]} — Kelly size too small: ${kelly.position_usd:.2f}")
            return None

        # Usar best_ask si está disponible y es mayor que el mid — asegura que cruzamos
        # el spread para obtener fills reales en órdenes FAK (Fill And Kill).
        # Si el scanner no tiene best_ask (0), usa market_price.
        best_ask = getattr(opportunity, "best_ask", 0) or 0
        if best_ask > opportunity.market_price:
            # Bump pequeño por encima del ask para capturar el nivel
            price = round(min(best_ask + 0.002, 0.99), 4)
        else:
            price = round(opportunity.market_price, 4)
        size_usd = kelly.position_usd

        # Para parity arb (YES+NO): ambos tokens se compran en cantidades iguales.
        # total_cost = p_yes + p_no, size_tokens = pares a comprar.
        if opportunity.side == "YES+NO" and opportunity.price_b > 0:
            total_price = price + opportunity.price_b
            size_tokens = round(size_usd / total_price, 2) if total_price > 0 else 0
        else:
            size_tokens = round(size_usd / price, 2) if price > 0 else 0

        # Polymarket CLOB requiere minimo 5 tokens por orden
        MIN_TOKENS = 5.0
        if size_tokens < MIN_TOKENS:
            min_usd = MIN_TOKENS * price
            if min_usd > self.max_position_usd:
                logger.debug(
                    f"[EXECUTOR] Skip {opportunity.condition_id[:12]} — "
                    f"min order ${min_usd:.2f} > max_position ${self.max_position_usd:.2f}"
                )
                return None
            size_tokens = MIN_TOKENS
            size_usd    = round(MIN_TOKENS * price, 2)

        if self.dry_run:
            result = self._simulate_trade(opportunity, price, size_tokens, size_usd)
        else:
            result = self._execute_trade(opportunity, price, size_tokens, size_usd)

        if result:
            # Solo guardar en CSV los fills exitosos — no registrar FAK rejections
            if result.success or result.dry_run:
                self._save_trade(result)
            # Solo marcar como ejecutado si el fill fue exitoso (o es simulación).
            # Una orden FAK rechazada por falta de liquidez NO debe bloquear el mercado,
            # pero sí activar cooldown para no spammear.
            if not result.success and not result.dry_run:
                self._failed_cooldown[opportunity.condition_id] = time.time()
            if result.success or result.dry_run:
                self._trades_today += 1
                self._executed_ids.add(opportunity.condition_id)
                self._executed_questions.append(
                    (opportunity.question, getattr(opportunity, "end_date", ""), opportunity.side)
                )
                self._save_state()
            # Notificar por Telegram cuando el trade es exitoso
            if result.success and self.notifier:
                try:
                    self.notifier.notify_trade_opened(
                        question=opportunity.question,
                        side=opportunity.side,
                        entry_price=result.price,
                        position_usd=result.position_usd,
                        signal_type=opportunity.signal_type.value,
                        edge=opportunity.edge,
                        dry_run=result.dry_run,
                        order_id=result.order_id,
                    )
                except Exception as _e:
                    logger.debug(f"[EXECUTOR] Telegram notify error: {_e}")
            # Contabilizar contra el limite diario de perdidas.
            # Usamos el precio de entrada como costo (perdida maxima posible si el trade
            # va a 0). No sumamos el size completo ya que ganamos cuando el mercado resuelve.
            # Esto evita que el kill switch se active despues de trades EXITOSOS.
            if not result.dry_run and result.success:
                self._daily_loss += result.position_usd
                logger.debug(f"[EXECUTOR] Daily exposure acumulada: ${self._daily_loss:.2f}/{self.max_daily_loss_usd}")

        return result

    def get_balance(self) -> float:
        """Retorna el balance de USDC disponible en la cuenta."""
        try:
            # sig_type=2 si opera via Gnosis Safe; sig_type=0 si es EOA directo
            sig = 2 if self._proxy_address else 0
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=sig)
            resp = self.client.get_balance_allowance(params)
            raw = resp.get("balance", 0) or resp.get("collateral_balance", 0)
            val = float(raw)
            # Balance viene en micro-USDC (6 decimales) si es > 10000
            return val / 1e6 if val > 10000 else val
        except Exception as e:
            logger.warning(f"get_balance failed: {e}")
            return 0.0

    # ── Internal ───────────────────────────────────────────────────────────────

    def _passes_filters(self, opp: Opportunity) -> bool:
        # Edge minimo para ejecutar (mas alto que para detectar)
        effective_min = self._adjusted_min_edge(opp)
        if opp.edge < effective_min:
            signal_name = opp.signal_type.value if hasattr(opp.signal_type, "value") else str(opp.signal_type)
            logger.debug(
                f"[EXECUTOR] Skip {opp.condition_id[:12]} — edge {opp.edge:.4f} < "
                f"threshold {effective_min:.4f} "
                f"(consensus={getattr(opp,'consensus_count',1)} "
                f"trend={getattr(opp,'price_trend',0.0):+.3f} "
                f"cal={self._signal_calibration.get(signal_name, 1.0):.2f})"
            )
            return False

        # Horizonte temporal por tipo de señal
        end_date = getattr(opp, "end_date", "") or ""
        if end_date:
            try:
                from datetime import timezone as _tz
                dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=_tz.utc)
                days_left = (dt - datetime.now(_tz.utc)).total_seconds() / 86400
                signal_name = opp.signal_type.value if hasattr(opp.signal_type, "value") else str(opp.signal_type)
                max_days = SIGNAL_MAX_DAYS.get(signal_name, self.max_days_to_resolution)
                if days_left > max_days:
                    logger.debug(
                        f"[EXECUTOR] Skip {opp.condition_id[:12]} — "
                        f"{signal_name} cierra en {days_left:.0f}d (max {max_days}d)"
                    )
                    return False
            except Exception:
                pass

        # Token ID requerido para operar
        if not opp.token_id:
            return False

        # Liquidez minima
        if opp.liquidity_usd < 2000:
            return False

        # Hard price floor/ceiling — no operar en extremos sin liquidez real
        # Rango principal: $0.05-$0.95. Para CALIBRATION_BIAS LOW case, permitir hasta $0.03
        # (el paper SSRN documenta edge en YES < 10%, necesitamos bajar a 3 centavos).
        signal_name = opp.signal_type.value if hasattr(opp.signal_type, "value") else str(opp.signal_type)
        price_floor = 0.03 if signal_name == "CALIBRATION_BIAS" else 0.05
        if opp.market_price < price_floor:
            logger.debug(f"[EXECUTOR] Skip {opp.condition_id[:12]} — precio {opp.market_price:.3f} bajo piso ${price_floor}")
            return False
        if opp.market_price > 0.95:
            logger.debug(f"[EXECUTOR] Skip {opp.condition_id[:12]} — precio {opp.market_price:.3f} sobre techo $0.95")
            return False

        # Cooldown después de FAK rejection — no spammear el mismo mercado
        cooldown_ts = self._failed_cooldown.get(opp.condition_id, 0)
        if time.time() - cooldown_ts < 300:  # 5 minutos de cooldown
            return False

        # No operar en el mismo mercado dos veces (ningún lado) — evita hedgearse a sí mismo
        if opp.condition_id in self._executed_ids:
            logger.debug(f"[EXECUTOR] Skip {opp.condition_id[:12]} — mercado ya operado hoy (bloqueo ambos lados)")
            return False

        # Mercados correlados: mismo evento real con condition_id distinto (ej: candidatos al mismo cargo)
        if self._is_correlated_market(opp):
            logger.debug(f"[EXECUTOR] Skip {opp.condition_id[:12]} — mercado correlado ya operado")
            return False

        # Freno de perdidas diarias
        if self._daily_loss >= self.max_daily_loss_usd:
            logger.warning(f"[EXECUTOR] Daily loss limit reached (${self._daily_loss:.2f}). No mas trades hoy.")
            return False

        return True

    def _is_correlated_market(self, opp: Opportunity) -> bool:
        """
        Detecta mercados mutuamente exclusivos que pertenecen al mismo evento real.

        Distingue self-hedge (malo) de mutex-arb (bueno):
          - Orban YES + Magyar YES  → SELF-HEDGE: solo uno puede ganar → bloquear
          - Scheffler NO + Rory NO  → MUTEX ARB: suma YES > 1 → ambos NOs ganan con alta prob → permitir
          - Orban YES + Magyar NO   → Posiciones opuestas en mutex → bloquear (dudoso)

        Regla: solo bloquear cuando la nueva trade es YES y ya hay un YES correlado,
        o cuando la nueva es YES y hay un NO correlado (y viceversa).
        Permite múltiples NO en el mismo grupo mutex (el caso del Masters con 79 candidatos).

        Solo se aplica a señales de correlación (MISPRICED_CORR, COMBINATORIAL_ARB, CALIBRATION_BIAS).
        """
        signal_name = opp.signal_type.value if hasattr(opp.signal_type, "value") else str(opp.signal_type)
        if signal_name not in ("MISPRICED_CORR", "COMBINATORIAL_ARB", "CALIBRATION_BIAS"):
            return False

        new_side = (opp.side or "").upper()
        end_date_new = (getattr(opp, "end_date", "") or "")[:10]  # solo YYYY-MM-DD
        stop = {"will", "the", "be", "a", "an", "of", "in", "on", "at", "to", "by",
                "is", "for", "or", "and", "not", "no", "?", "next"}
        words_new = set(opp.question.lower().split()) - stop

        for entry in self._executed_questions:
            # Soporte backward-compatible: tuplas viejas (q, date) y nuevas (q, date, side)
            if len(entry) >= 3:
                prev_q, prev_date, prev_side = entry[0], entry[1], (entry[2] or "").upper()
            else:
                prev_q, prev_date = entry[0], entry[1]
                prev_side = ""

            # Misma ventana temporal (±7 días)
            if end_date_new and prev_date:
                try:
                    from datetime import date as _date
                    d1 = _date.fromisoformat(end_date_new)
                    d2 = _date.fromisoformat(prev_date[:10])
                    if abs((d1 - d2).days) > 7:
                        continue
                except Exception:
                    pass

            words_prev = set(prev_q.lower().split()) - stop
            if not words_new or not words_prev:
                continue

            intersection = words_new & words_prev
            union        = words_new | words_prev
            jaccard = len(intersection) / len(union) if union else 0.0

            if jaccard < 0.45:
                continue

            # Ambos NO en grupo mutex → arb legítimo, permitir
            if new_side == "NO" and prev_side == "NO":
                continue

            # En cualquier otro caso (YES+YES, YES+NO, NO+YES, o sin side previo) → bloquear
            logger.debug(
                f"[EXECUTOR] Mercado correlado (Jaccard={jaccard:.2f}, "
                f"sides={prev_side}/{new_side}): '{opp.question[:40]}' ~ '{prev_q[:40]}'"
            )
            return True

        return False

    def _simulate_trade(self, opp: Opportunity, price: float, size: float, size_usd: float) -> TradeResult:
        logger.info(
            f"[DRY RUN] {opp.signal_type.value} | {opp.question[:50]} | "
            f"side={opp.side} price={price:.3f} size=${size_usd:.2f} edge={opp.edge:.4f}"
        )
        return TradeResult(
            opportunity=opp,
            position_usd=size_usd,
            price=price,
            size=size,
            order_id="DRY_RUN",
            dry_run=True,
            success=True,
        )

    def _execute_trade(self, opp: Opportunity, price: float, size: float, size_usd: float) -> TradeResult:
        try:
            if opp.side == "YES+NO" and opp.token_id_b and opp.price_b > 0:
                return self._execute_parity_trade(opp, price, size, size_usd)

            order_args = OrderArgs(
                token_id=opp.token_id,
                price=price,
                size=size,
                side="BUY",
            )
            signed = self.client.create_order(order_args)
            resp   = self.client.post_order(signed, OrderType.FAK)

            order_id = resp.get("orderID", "") or resp.get("id", "")
            success  = resp.get("success", False) or bool(order_id)

            if success:
                logger.info(
                    f"[TRADE FILLED] {opp.signal_type.value} | {opp.question[:50]} | "
                    f"side={opp.side} price={price:.3f} size=${size_usd:.2f} "
                    f"edge={opp.edge:.4f} order_id={order_id}"
                )
            else:
                logger.warning(
                    f"[TRADE NO FILL] {opp.question[:50]} | "
                    f"side={opp.side} price={price:.3f} — sin liquidez a ese precio"
                )

            return TradeResult(
                opportunity=opp,
                position_usd=size_usd,
                price=price,
                size=size,
                order_id=order_id,
                dry_run=False,
                success=success,
                error="" if success else str(resp),
            )

        except Exception as e:
            err_str = str(e)
            status  = getattr(e, "status_code", None)
            err_msg = getattr(e, "error_msg", err_str)
            logger.error(f"[TRADE ERROR] {opp.condition_id[:12]}: status={status} {err_msg}")
            if status == 403 or "restricted" in str(err_msg).lower():
                logger.warning(
                    "[EXECUTOR] Geoblock 403 detectado — trading restringido en esta region. "
                    "Activando kill switch. Revisá el proxy en HTTPS_PROXY."
                )
                self._daily_loss = self.max_daily_loss_usd
            elif "not enough balance" in err_str.lower() or "allowance" in err_str.lower():
                logger.warning(
                    "[EXECUTOR] Balance/allowance insuficiente detectado — "
                    "activando kill switch diario para evitar spam de ordenes fallidas."
                )
                self._daily_loss = self.max_daily_loss_usd
            return TradeResult(
                opportunity=opp,
                position_usd=size_usd,
                price=price,
                size=size,
                order_id="",
                dry_run=False,
                success=False,
                error=err_str,
            )

    def _execute_parity_trade(self, opp: Opportunity, price_yes: float, size: float, size_usd: float) -> TradeResult:
        """
        Parity arb: compra YES y NO en igual cantidad de tokens.
        size = pares a comprar. price_yes = p_yes, opp.price_b = p_no.
        Gasto real: size * (p_yes + p_no) = size_usd.
        """
        order_id_yes = ""
        order_id_no  = ""
        errors       = []

        try:
            yes_args = OrderArgs(token_id=opp.token_id,   price=price_yes,    size=size, side="BUY")
            signed   = self.client.create_order(yes_args)
            resp     = self.client.post_order(signed, OrderType.FAK)
            order_id_yes = resp.get("orderID", "") or resp.get("id", "")
            if not (resp.get("success", False) or order_id_yes):
                errors.append(f"YES failed: {resp}")
            else:
                logger.info(f"[PARITY YES] {opp.question[:45]} price={price_yes:.3f} size={size} order={order_id_yes}")
        except Exception as e:
            errors.append(f"YES error: {e}")
            logger.error(f"[PARITY YES ERROR] {e}")

        try:
            no_args = OrderArgs(token_id=opp.token_id_b, price=opp.price_b, size=size, side="BUY")
            signed  = self.client.create_order(no_args)
            resp    = self.client.post_order(signed, OrderType.FAK)
            order_id_no = resp.get("orderID", "") or resp.get("id", "")
            if not (resp.get("success", False) or order_id_no):
                errors.append(f"NO failed: {resp}")
            else:
                logger.info(f"[PARITY NO]  {opp.question[:45]} price={opp.price_b:.3f} size={size} order={order_id_no}")
        except Exception as e:
            errors.append(f"NO error: {e}")
            logger.error(f"[PARITY NO ERROR] {e}")

        success = bool(order_id_yes and order_id_no and not errors)
        combined_id = f"{order_id_yes}|{order_id_no}"
        if success:
            logger.info(
                f"[PARITY] {opp.question[:50]} | "
                f"p_yes={price_yes:.3f} p_no={opp.price_b:.3f} "
                f"pairs={size} cost=${size_usd:.2f} edge={opp.edge:.4f}"
            )

        return TradeResult(
            opportunity=opp,
            position_usd=size_usd,
            price=price_yes + opp.price_b,   # total cost per pair
            size=size,
            order_id=combined_id,
            dry_run=False,
            success=success,
            error="; ".join(errors),
        )

    # ── Persistencia de estado diario ─────────────────────────────────────────

    def _load_state(self):
        """
        Restaura el estado al iniciar:
        - executed_ids / executed_questions: SIEMPRE se cargan (independiente del día),
          porque las posiciones abiertas duran hasta que el mercado resuelve, no 1 día.
          Se podan los que ya resolvieron via _prune_resolved_ids().
        - daily_loss / trades_today: solo si es del día actual (son contadores diarios).
        """
        if not os.path.exists(self._state_file):
            return
        try:
            with open(self._state_file, encoding="utf-8") as f:
                data = json.load(f)
            # Cargar executed_ids SIEMPRE — evita self-hedge cross-día
            self._executed_ids = set(data.get("executed_ids", []))
            self._executed_questions = data.get("executed_questions", [])
            # Podar condition_ids que ya resolvieron
            self._prune_resolved_ids()
            # Contadores diarios: solo si es del día actual
            saved_date_str = data.get("date", "")
            try:
                saved_date = date.fromisoformat(saved_date_str)
                if saved_date == date.today():
                    self._daily_loss = float(data.get("daily_loss", 0.0))
            except (ValueError, TypeError):
                pass
            n = len(self._executed_ids)
            if n:
                logger.info(
                    f"[EXECUTOR] Estado restaurado: {n} mercados con posicion activa, "
                    f"daily_loss=${self._daily_loss:.2f}"
                )
        except Exception as e:
            logger.debug(f"[EXECUTOR] No se pudo cargar estado: {e}")

    def _prune_resolved_ids(self):
        """
        Saca de executed_ids y executed_questions los condition_ids cuyos mercados
        ya resolvieron (status won/lost en pnl_state.json). Esto permite reusar el
        capital de posiciones cerradas sin riesgo de self-hedge en las abiertas.
        """
        pnl_path = "data/pnl_state.json"
        if not os.path.exists(pnl_path):
            return
        try:
            with open(pnl_path, encoding="utf-8") as f:
                pnl_state = json.load(f)
            resolved = {
                cid for cid, s in pnl_state.items()
                if s.get("status") in ("won", "lost")
            }
            before = len(self._executed_ids)
            self._executed_ids = {cid for cid in self._executed_ids if cid not in resolved}
            # executed_questions no tiene el cid pero no importa — los datos viejos
            # se podan por la ventana ±7d en _is_correlated_market
            after = len(self._executed_ids)
            if before != after:
                logger.info(f"[EXECUTOR] Podados {before - after} mercados resueltos de executed_ids")
        except Exception as e:
            logger.debug(f"[EXECUTOR] No se pudo podar executed_ids: {e}")

    def _refresh_calibration(self):
        """
        Computa factores de calibración por tipo de señal a partir del historial
        de trades reales (trades.csv + pnl_state.json).

        Factor de calibración:
          - 1.0  = neutral (sin historial o win rate = 50%)
          - <1.0 = señal confiable (win rate > 50%) → umbral más fácil de alcanzar
          - >1.0 = señal poco confiable (win rate < 50%) → umbral más exigente

        Fórmula: multiplier = 1.0 - (win_rate - 0.5) * 0.80
        Cap: [0.70, 1.40]
        Mínimo 8 trades resueltos por señal para aplicar calibración.
        """
        cal = {}
        try:
            # Leer trades
            if not os.path.exists(self.trades_csv):
                self._signal_calibration = cal
                return

            import csv as _csv
            trades_by_signal: dict = {}
            with open(self.trades_csv, "r", encoding="utf-8") as f:
                for row in _csv.DictReader(f):
                    if row.get("dry_run", "True").lower() == "true":
                        continue
                    if row.get("success", "False").lower() != "true":
                        continue
                    # Filtrar trades fantasma: ordenes GTC a precios extremos que nunca
                    # se llenaron (era el bug pre-FOK). Solo contar trades a precios donde
                    # realmente hay liquidez ejecutable.
                    try:
                        price = float(row.get("price", "0"))
                        if price < 0.05 or price > 0.95:
                            continue
                    except (ValueError, TypeError):
                        continue
                    cid = row.get("condition_id", "")
                    sig = row.get("signal_type", "UNKNOWN")
                    if cid and sig:
                        trades_by_signal.setdefault(sig, []).append(cid)

            # Leer estado de resolución
            pnl_state_path = "data/pnl_state.json"
            if not os.path.exists(pnl_state_path):
                self._signal_calibration = cal
                return

            with open(pnl_state_path, "r", encoding="utf-8") as f:
                pnl_state = json.load(f)

            # Calcular win rate por señal
            for sig, cids in trades_by_signal.items():
                wins = losses = 0
                seen = set()
                for cid in cids:
                    if cid in seen:
                        continue
                    seen.add(cid)
                    status = pnl_state.get(cid, {}).get("status", "open")
                    if status == "won":
                        wins += 1
                    elif status == "lost":
                        losses += 1

                resolved = wins + losses
                if resolved < 8:
                    continue   # historial insuficiente

                win_rate = wins / resolved
                multiplier = 1.0 - (win_rate - 0.5) * 0.80
                multiplier = max(0.70, min(1.40, multiplier))
                cal[sig] = round(multiplier, 3)
                logger.info(
                    f"[CALIBRATION] {sig}: win_rate={win_rate:.0%} "
                    f"({wins}W/{losses}L) → multiplier={multiplier:.3f}"
                )

        except Exception as e:
            logger.debug(f"[CALIBRATION] Error calculando calibración: {e}")

        self._signal_calibration = cal

    def _adjusted_min_edge(self, opp: Opportunity) -> float:
        """
        Calcula el edge mínimo requerido para ejecutar, ajustado por:
        1. Consenso multi-señal (más señales acordando → umbral más bajo)
        2. Trayectoria de precio (precio moviéndose en contra → umbral más alto)
        3. Calibración histórica de la señal (win rate real → ajusta umbral)

        Retorna el threshold efectivo a comparar contra opp.edge.
        """
        signal_name = opp.signal_type.value if hasattr(opp.signal_type, "value") else str(opp.signal_type)
        base = SIGNAL_MIN_EDGE.get(signal_name, self.min_edge_to_trade)

        # ── 1. Bonus por consenso multi-señal ────────────────────────────────
        consensus = getattr(opp, "consensus_count", 1)
        if consensus >= 3:
            base *= 0.75    # 3+ señales acordando: umbral 25% más fácil
        elif consensus >= 2:
            base *= 0.875   # 2 señales: umbral 12.5% más fácil

        # ── 2. Penalización por precio en contra ──────────────────────────────
        trend = getattr(opp, "price_trend", 0.0)
        if trend < -0.015:
            base *= 1.25    # precio cayendo fuerte en nuestra contra: +25% de exigencia
        elif trend < -0.005:
            base *= 1.10    # tendencia leve en contra: +10%

        # ── 3. Calibración histórica de la señal ─────────────────────────────
        # Refrescar calibración cada 10 trades nuevos
        if self._trades_today - self._calibration_trades_count >= 10:
            self._refresh_calibration()
            self._calibration_trades_count = self._trades_today

        signal_name = opp.signal_type.value if hasattr(opp.signal_type, "value") else str(opp.signal_type)
        cal_multiplier = self._signal_calibration.get(signal_name, 1.0)
        base *= cal_multiplier

        return base

    def _save_state(self):
        """Persiste executed_ids y daily_loss a disco."""
        os.makedirs("data", exist_ok=True)
        try:
            with open(self._state_file, "w", encoding="utf-8") as f:
                json.dump({
                    "date":               date.today().isoformat(),
                    "executed_ids":       list(self._executed_ids),
                    "daily_loss":         round(self._daily_loss, 4),
                    "executed_questions": self._executed_questions,
                }, f)
        except Exception as e:
            logger.debug(f"[EXECUTOR] No se pudo guardar estado: {e}")

    def _ensure_csv(self):
        os.makedirs("data", exist_ok=True)
        if not os.path.exists(self.trades_csv):
            with open(self.trades_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "executed_at", "dry_run", "success", "signal_type",
                    "condition_id", "token_id", "question", "side",
                    "price", "size_tokens", "position_usd",
                    "edge", "fair_value", "order_id", "error"
                ])

    def _save_trade(self, result: TradeResult):
        with open(self.trades_csv, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            opp = result.opportunity
            writer.writerow([
                result.executed_at.isoformat(),
                result.dry_run,
                result.success,
                opp.signal_type.value,
                opp.condition_id,
                opp.token_id,
                opp.question,
                opp.side,
                result.price,
                result.size,
                result.position_usd,
                opp.edge,
                opp.fair_value,
                result.order_id,
                result.error,
            ])
