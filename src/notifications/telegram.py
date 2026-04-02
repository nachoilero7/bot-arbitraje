"""
Notificaciones via Telegram Bot.

Setup (5 minutos):
  1. Abrí Telegram y buscá @BotFather
  2. Escribí /newbot → seguí los pasos → copiá el TOKEN
  3. Escribí /start al bot que creaste
  4. Visitá https://api.telegram.org/bot{TOKEN}/getUpdates
     y copiá el "id" de dentro de "chat" → ese es tu CHAT_ID
  5. Agregá al .env:
       TELEGRAM_BOT_TOKEN=123456789:ABCdef...
       TELEGRAM_CHAT_ID=123456789

Mensajes enviados:
  - Al iniciar el bot (send_startup)
  - Cuando se ejecuta un trade, real o simulado (notify_trade_opened)
  - Cuando un trade cierra/resuelve con ganancia o pérdida (notify_trade_closed)
"""
import requests
from src.utils.logger import get_logger

logger = get_logger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


class TelegramNotifier:

    def __init__(self, bot_token: str, chat_id: str, min_notify_edge: float = 0.05):
        self.bot_token = bot_token
        self.chat_id   = chat_id
        self.min_notify_edge = min_notify_edge

    def notify_trade_opened(
        self,
        question: str,
        side: str,
        entry_price: float,
        position_usd: float,
        signal_type: str,
        edge: float,
        dry_run: bool = False,
        order_id: str = "",
    ) -> None:
        """
        Notifica cuando se posiciona un trade.
        Llamar siempre que TradeExecutor ejecute (o simule) un trade exitoso.
        """
        prefix = "[SIMULADO] " if dry_run else ""
        pct    = entry_price * 100

        lines = [
            f"{prefix}TRADE ABIERTO",
            f"",
            f"{question[:80]}",
            f"",
            f"Lado:      {side}",
            f"Entrada:   {entry_price:.3f}  ({pct:.1f}%)",
            f"Invertido: ${position_usd:.2f}",
            f"Signal:    {signal_type}",
            f"Edge:      +{edge:.1%}",
        ]
        if order_id and not dry_run:
            lines.append(f"Order ID:  {order_id}")

        self._send("\n".join(lines))

    def notify_trade_closed(
        self,
        question: str,
        side: str,
        entry_price: float,
        position_usd: float,
        pnl: float,
        won: bool,
        dry_run: bool = False,
    ) -> None:
        """
        Notifica cuando un trade cierra con resultado conocido (won / lost).
        Llamar cuando PnLTracker detecta que un mercado resolvió.
        """
        prefix = "[SIMULADO] " if dry_run else ""
        roi    = pnl / position_usd if position_usd > 0 else 0
        result = "GANADO" if won else "PERDIDO"
        pnl_str = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
        roi_str = f"+{roi:.1%}" if roi >= 0 else f"{roi:.1%}"

        lines = [
            f"{prefix}{result}",
            f"",
            f"{question[:80]}",
            f"",
            f"Lado:      {side}  |  Entrada: {entry_price:.3f}",
            f"P&L:       {pnl_str}",
            f"ROI:       {roi_str}",
            f"Invertido: ${position_usd:.2f}",
        ]
        self._send("\n".join(lines))

    def send_startup(self, interval: int, signals: list) -> None:
        """Mensaje de inicio cuando arranca el scanner."""
        text = (
            f"polyedge iniciado\n\n"
            f"Intervalo: {interval}s\n"
            f"Signals activos: {len(signals)}\n"
            f"Notificaciones: solo trades ejecutados y cierres"
        )
        self._send(text)

    def _send(self, text: str) -> bool:
        url = TELEGRAM_API.format(token=self.bot_token)
        try:
            resp = requests.post(url, json={
                "chat_id": self.chat_id,
                "text":    text,
                "disable_web_page_preview": True,
            }, timeout=10)
            resp.raise_for_status()
            logger.info(f"Telegram: {text[:60].replace(chr(10),' ')}...")
            return True
        except Exception as e:
            logger.warning(f"Telegram send failed: {e}")
            return False
