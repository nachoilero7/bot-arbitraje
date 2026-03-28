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

El bot te manda un mensaje cada vez que detecta edge >= min_notify_edge.
"""
import requests
from src.utils.logger import get_logger

logger = get_logger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


class TelegramNotifier:

    def __init__(self, bot_token: str, chat_id: str, min_notify_edge: float = 0.05):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.min_notify_edge = min_notify_edge
        self._last_notified: set[str] = set()  # evitar duplicados por scan

    def notify_opportunities(self, opportunities: list, scan_num: int) -> None:
        """Manda una notificacion por cada oportunidad nueva que supere el umbral."""
        for opp in opportunities:
            if opp.edge < self.min_notify_edge:
                continue
            # Clave de deduplicacion: condition_id + side + precio redondeado
            key = f"{opp.condition_id}:{opp.side}:{opp.market_price:.2f}"
            if key in self._last_notified:
                continue

            self._send(self._format_message(opp, scan_num))
            self._last_notified.add(key)

        # Limpiar cache de notificaciones cada 100 oportunidades unicas
        if len(self._last_notified) > 200:
            self._last_notified.clear()

    def _format_message(self, opp, scan_num: int) -> str:
        liq = f"${opp.liquidity_usd:,.0f}" if opp.liquidity_usd else "N/A"
        vol = f"${opp.volume_24h:,.0f}" if opp.volume_24h else "N/A"

        lines = [
            f"EDGE DETECTADO - Scan #{scan_num}",
            f"",
            f"Signal: {opp.signal_type.value}",
            f"{opp.question[:80]}",
            f"",
            f"Side: {opp.side} @ {opp.market_price:.3f}",
            f"Edge: +{opp.edge:.4f} ({opp.edge_pct:.1%})",
            f"Fair value: {opp.fair_value:.3f}",
            f"",
            f"Liquidity: {liq} | Vol 24h: {vol}",
            f"Spread: {opp.spread:.3f}",
        ]

        if opp.notes:
            lines += ["", opp.notes[:120]]

        return "\n".join(lines)

    def _send(self, text: str) -> bool:
        url = TELEGRAM_API.format(token=self.bot_token)
        try:
            resp = requests.post(url, json={
                "chat_id": self.chat_id,
                "text": text,
                "disable_web_page_preview": True,
            }, timeout=10)
            resp.raise_for_status()
            logger.info(f"Telegram notified: {text[:60]}...")
            return True
        except Exception as e:
            logger.warning(f"Telegram send failed: {e}")
            return False

    def send_startup(self, interval: int, signals: list[str]) -> None:
        """Manda mensaje de inicio cuando arranca el scanner."""
        text = (
            f"polyedge arranco\n\n"
            f"Intervalo: {interval}s\n"
            f"Signals: {', '.join(signals)}\n"
            f"Notificando edges >= {self.min_notify_edge:.0%}"
        )
        self._send(text)

    def send_summary(self, total_opps: int, top_edge: float, scan_num: int) -> None:
        """Resumen cada 10 scans."""
        if total_opps > 0:
            text = (
                f"Resumen - Scan #{scan_num}\n\n"
                f"Total oportunidades: {total_opps}\n"
                f"Mejor edge: +{top_edge:.4f}"
            )
        else:
            text = f"Resumen - Scan #{scan_num}\n\nSin oportunidades destacadas."
        self._send(text)
