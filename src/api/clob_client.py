"""
Clientes para las dos APIs de Polymarket:

- GammaApiClient: mercados activos con precios incluidos (bid/ask/spread/liquidity)
  Endpoint: https://gamma-api.polymarket.com
  Usar para: discovery de mercados, precios actuales, filtrado por liquidez

- ClobApiClient: orderbook detallado y ejecucion de trades
  Endpoint: https://clob.polymarket.com
  Usar para: orderbook completo, Phase 2 ejecucion
"""
import requests
import time
from src.utils.logger import get_logger

logger = get_logger(__name__)


class GammaApiClient:
    """
    Cliente Gamma API — mercados activos con precios incluidos.
    No requiere autenticacion.
    """

    BASE_URL = "https://gamma-api.polymarket.com"

    def __init__(self, timeout: int = 10, max_retries: int = 3, proxy: str = None):
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        if proxy:
            self.session.proxies.update({"http": proxy, "https": proxy})
            logger.info(f"Using proxy: {proxy}")

    def _get(self, path: str, params: dict = None) -> list | dict:
        url = f"{self.BASE_URL}{path}"
        for attempt in range(self.max_retries):
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as e:
                logger.warning(f"Request failed ({attempt + 1}/{self.max_retries}): {url} - {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(1 * (attempt + 1))
        raise RuntimeError(f"Max retries reached for {url}")

    def get_active_markets(
        self,
        limit: int = 500,
        offset: int = 0,
        min_liquidity: float = 0,
    ) -> list[dict]:
        """
        Retorna mercados activos con precios ya incluidos.
        Cada mercado incluye: bestBid, bestAsk, spread, outcomePrices, liquidityNum, volume24hr, clobTokenIds.
        """
        params = {
            "active": "true",
            "closed": "false",
            "acceptingOrders": "true",
            "limit": limit,
            "offset": offset,
        }
        markets = self._get("/markets", params=params)
        if min_liquidity > 0:
            markets = [m for m in markets if (m.get("liquidityNum") or 0) >= min_liquidity]
        return markets

    def get_all_active_markets(self, min_liquidity: float = 500, max_markets: int = 5000) -> list[dict]:
        """
        Itera paginas y retorna todos los mercados activos con liquidez minima.
        Ordena por liquidez descendente para procesar los mejores primero.
        """
        all_markets = []
        offset = 0
        page_size = 500

        while len(all_markets) < max_markets:
            page = self.get_active_markets(limit=page_size, offset=offset, min_liquidity=min_liquidity)
            if not page:
                break
            all_markets.extend(page)
            offset += page_size
            # Si la pagina vino incompleta, llegamos al final
            if len(page) < page_size:
                break

        # Ordenar por liquidez desc para priorizar mercados con mas accion
        all_markets.sort(key=lambda m: m.get("liquidityNum") or 0, reverse=True)
        logger.info(f"Fetched {len(all_markets)} active markets (min_liquidity=${min_liquidity})")
        return all_markets[:max_markets]


class ClobApiClient:
    """
    Cliente CLOB API — orderbook detallado.
    Phase 1: solo lectura (L0).
    """

    BASE_URL = "https://clob.polymarket.com"

    def __init__(self, timeout: int = 10, max_retries: int = 3, proxy: str = None):
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        if proxy:
            self.session.proxies.update({"http": proxy, "https": proxy})

    def _get(self, path: str, params: dict = None) -> dict | list:
        url = f"{self.BASE_URL}{path}"
        for attempt in range(self.max_retries):
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as e:
                logger.warning(f"Request failed ({attempt + 1}/{self.max_retries}): {url} - {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(1 * (attempt + 1))
        raise RuntimeError(f"Max retries reached for {url}")

    def get_orderbook(self, token_id: str) -> dict:
        return self._get("/book", params={"token_id": token_id})

    def get_price(self, token_id: str, side: str = "BUY") -> float:
        data = self._get("/price", params={"token_id": token_id, "side": side})
        return float(data.get("price", 0))
