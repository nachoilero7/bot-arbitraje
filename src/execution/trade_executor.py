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
import os
import threading
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
        dry_run: bool = True,
        trades_csv: str = "data/trades.csv",
        proxy_address: str = None,         # Gnosis Safe (funder) — POLY_GNOSIS_SAFE mode
    ):
        self.bankroll_usd      = bankroll_usd
        self.min_edge_to_trade = min_edge_to_trade
        self.max_position_usd  = max_position_usd
        self.max_daily_loss_usd = max_daily_loss_usd
        self.kelly_fraction    = kelly_fraction
        self.dry_run           = dry_run
        self.trades_csv        = trades_csv

        self._proxy_address    = proxy_address   # None = EOA, str = POLY_GNOSIS_SAFE
        self._daily_loss: float = 0.0
        self._loss_date: date   = date.today()
        self._trades_today: int = 0
        self._executed_ids: set = set()  # evitar duplicados por condition_id+side
        self._lock = threading.Lock()    # serializa maybe_execute entre threads

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
        # Reset contador diario si cambio el dia
        if date.today() != self._loss_date:
            self._daily_loss  = 0.0
            self._loss_date   = date.today()
            self._trades_today = 0

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

        price    = opportunity.market_price
        size_usd = kelly.position_usd

        # Para parity arb (YES+NO): ambos tokens se compran en cantidades iguales.
        # total_cost = p_yes + p_no, size_tokens = pares a comprar.
        if opportunity.side == "YES+NO" and opportunity.price_b > 0:
            total_price = price + opportunity.price_b
            size_tokens = round(size_usd / total_price, 2) if total_price > 0 else 0
        else:
            size_tokens = round(size_usd / price, 2) if price > 0 else 0

        if self.dry_run:
            result = self._simulate_trade(opportunity, price, size_tokens, size_usd)
        else:
            result = self._execute_trade(opportunity, price, size_tokens, size_usd)

        if result:
            self._save_trade(result)
            self._trades_today += 1
            # Marcar como ejecutado para no repetir
            self._executed_ids.add(f"{opportunity.condition_id}:{opportunity.side}")
            # Contabilizar el gasto contra el limite diario de perdidas
            # (posicion completa como perdida potencial maxima — conservador pero seguro)
            if not result.dry_run and result.success:
                self._daily_loss += result.position_usd
                logger.debug(f"[EXECUTOR] Daily spend acumulado: ${self._daily_loss:.2f}/{self.max_daily_loss_usd}")

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
        if opp.edge < self.min_edge_to_trade:
            return False

        # Token ID requerido para operar
        if not opp.token_id:
            return False

        # Liquidez minima
        if opp.liquidity_usd < 500:
            return False

        # Filtrar mercados con precio >= 0.99 (ya resueltos o en proceso, CLOB rechaza > 0.999)
        if opp.market_price >= 0.99:
            logger.debug(f"[EXECUTOR] Skip {opp.condition_id[:12]} — precio {opp.market_price:.4f} demasiado alto (mercado resuelto)")
            return False

        # No repetir el mismo trade
        trade_key = f"{opp.condition_id}:{opp.side}"
        if trade_key in self._executed_ids:
            logger.debug(f"[EXECUTOR] Skip {opp.condition_id[:12]} — ya ejecutado hoy")
            return False

        # Freno de perdidas diarias
        if self._daily_loss >= self.max_daily_loss_usd:
            logger.warning(f"[EXECUTOR] Daily loss limit reached (${self._daily_loss:.2f}). No mas trades hoy.")
            return False

        return True

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
            resp   = self.client.post_order(signed, OrderType.GTC)

            order_id = resp.get("orderID", "") or resp.get("id", "")
            success  = resp.get("success", False) or bool(order_id)

            if success:
                logger.info(
                    f"[TRADE] {opp.signal_type.value} | {opp.question[:50]} | "
                    f"side={opp.side} price={price:.3f} size=${size_usd:.2f} "
                    f"edge={opp.edge:.4f} order_id={order_id}"
                )
            else:
                logger.error(f"[TRADE FAILED] {resp}")

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
            logger.error(f"[TRADE ERROR] {opp.condition_id[:12]}: {e}")
            if "not enough balance" in err_str.lower() or "allowance" in err_str.lower():
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
            resp     = self.client.post_order(signed, OrderType.GTC)
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
            resp    = self.client.post_order(signed, OrderType.GTC)
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
