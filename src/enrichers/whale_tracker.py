"""
Enricher: Whale Tracker
Detecta actividad de "ballenas" (grandes operadores) en Polymarket CLOB API.

Endpoint publico:
  GET https://clob.polymarket.com/trades?token_id=TOKEN_ID&limit=50

Respuesta: lista de trades con trader_side, price, size (USDC), timestamp.

Uso:
  tracker = WhaleTracker()
  pressure = tracker.get_whale_pressure(token_id)
  # {"buy_volume": 12000, "sell_volume": 4000, "ratio": 0.75, "whale_count": 5}
"""
import time
import requests
from src.utils.logger import get_logger

logger = get_logger(__name__)

BASE_URL = "https://clob.polymarket.com"
CACHE_TTL_SECS = 120   # 2 minutos
WHALE_SIZE_USD  = 2000  # Tamano minimo (USDC) para considerar una operacion "whale"


class WhaleTracker:
    """
    Consulta trades recientes de Polymarket CLOB y detecta presion
    compradora/vendedora de grandes operadores.
    """

    def __init__(self, timeout: int = 10):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        # token_id -> (timestamp, list[dict])
        self._cache: dict[str, tuple[float, list]] = {}

    # ── Public API ─────────────────────────────────────────────────────────────

    def get_recent_trades(self, token_id: str) -> list[dict]:
        """
        Retorna los ultimos 50 trades del token dado.
        Resultado cacheado por CACHE_TTL_SECS segundos.
        """
        now = time.time()

        if token_id in self._cache:
            ts, cached = self._cache[token_id]
            if now - ts < CACHE_TTL_SECS:
                return cached

        try:
            resp = self.session.get(
                f"{BASE_URL}/trades",
                params={"token_id": token_id, "limit": 50},
                timeout=self.timeout,
            )
            resp.raise_for_status()
            data = resp.json()
            # El endpoint puede devolver la lista directamente o dentro de "data"
            trades = data if isinstance(data, list) else data.get("data", [])
            self._cache[token_id] = (now, trades)
            return trades
        except Exception as e:
            logger.warning(f"WhaleTracker: error fetching trades for {token_id[:12]}: {e}")
            # Devolver cache vencido si existe, si no lista vacia
            if token_id in self._cache:
                _, stale = self._cache[token_id]
                return stale
            return []

    def get_whale_pressure(self, token_id: str) -> dict:
        """
        Analiza los trades recientes y calcula la presion de ballenas.

        Returns:
            {
                "buy_volume":  float,  # USDC comprado por ballenas
                "sell_volume": float,  # USDC vendido por ballenas
                "ratio":       float,  # buy / (buy + sell), 0.5 si no hay datos
                "whale_count": int,    # cantidad de trades whale encontrados
            }
        """
        empty = {"buy_volume": 0.0, "sell_volume": 0.0, "ratio": 0.5, "whale_count": 0}

        try:
            trades = self.get_recent_trades(token_id)
            if not trades:
                return empty

            buy_volume  = 0.0
            sell_volume = 0.0
            whale_count = 0

            for trade in trades:
                try:
                    size = float(trade.get("size", 0) or 0)
                    if size < WHALE_SIZE_USD:
                        continue

                    side = str(trade.get("trader_side", "")).upper()
                    if side == "BUY":
                        buy_volume += size
                    elif side == "SELL":
                        sell_volume += size
                    else:
                        # Lado desconocido: ignorar
                        continue

                    whale_count += 1
                except (TypeError, ValueError):
                    continue

            total = buy_volume + sell_volume
            ratio = (buy_volume / total) if total > 0 else 0.5

            return {
                "buy_volume":  round(buy_volume, 2),
                "sell_volume": round(sell_volume, 2),
                "ratio":       round(ratio, 4),
                "whale_count": whale_count,
            }

        except Exception as e:
            logger.warning(f"WhaleTracker: error computing pressure for {token_id[:12]}: {e}")
            return empty
