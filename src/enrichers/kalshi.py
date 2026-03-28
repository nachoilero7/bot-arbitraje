"""
Enricher: Kalshi API
https://trading.kalshi.com — mercado de prediccion regulado (CFTC)

Busca los mismos eventos en Kalshi para detectar arbitraje cruzado:
  Polymarket YES = 0.45, Kalshi YES = 0.52 → comprar en Polymarket

Kalshi usa cents (0-100) para precios, Polymarket usa decimales (0-1).
API publica, no requiere auth para leer mercados.

Endpoints:
  GET /trade-api/v2/markets?limit=200&status=open  → mercados activos
  GET /trade-api/v2/markets/{ticker}               → detalle de un mercado
"""
import time
import requests
from src.utils.logger import get_logger

logger = get_logger(__name__)

BASE_URL = "https://api.elections.kalshi.com"
CACHE_TTL_SECS = 60


class KalshiApiClient:

    def __init__(self, timeout: int = 10):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        self._cache: dict[str, tuple[float, list]] = {}

    def _get(self, path: str, params: dict = None) -> dict | list:
        url = f"{BASE_URL}{path}"
        try:
            resp = self.session.get(url, params=params or {}, timeout=self.timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.error(f"Kalshi API error: {url} — {e}")
            raise

    def get_active_markets(self, limit: int = 1000) -> list[dict]:
        cache_key = f"markets_{limit}"
        now = time.time()

        if cache_key in self._cache:
            ts, data = self._cache[cache_key]
            if now - ts < CACHE_TTL_SECS:
                return data

        markets = []
        cursor = None
        page_size = min(limit, 200)

        try:
            while len(markets) < limit:
                params = {"limit": page_size, "status": "open"}
                if cursor:
                    params["cursor"] = cursor

                data = self._get("/trade-api/v2/markets", params=params)
                raw_markets = data.get("markets", []) if isinstance(data, dict) else []

                for m in raw_markets:
                    markets.append(self._normalize(m))

                cursor = data.get("cursor") if isinstance(data, dict) else None
                if not cursor or not raw_markets:
                    break

            self._cache[cache_key] = (now, markets)
            logger.info(f"Kalshi: fetched {len(markets)} active markets")
        except Exception as e:
            logger.warning(f"Kalshi fetch failed: {e}")
            if cache_key in self._cache:
                _, stale = self._cache[cache_key]
                return stale

        return markets

    def _normalize(self, m: dict) -> dict:
        ticker = m.get("ticker", "")

        yes_bid_cents = m.get("yes_bid", 0) or 0
        yes_ask_cents = m.get("yes_ask", 0) or 0
        no_bid_cents  = m.get("no_bid", 0) or 0
        no_ask_cents  = m.get("no_ask", 0) or 0

        yes_bid = yes_bid_cents / 100
        yes_ask = yes_ask_cents / 100
        no_bid  = no_bid_cents  / 100
        no_ask  = no_ask_cents  / 100

        yes_mid = (yes_bid + yes_ask) / 2 if (yes_bid or yes_ask) else 0.0

        return {
            "ticker":    ticker,
            "title":     m.get("title", ""),
            "yes_price": round(yes_mid, 4),
            "yes_bid":   yes_bid,
            "yes_ask":   yes_ask,
            "no_bid":    no_bid,
            "no_ask":    no_ask,
            "volume":    m.get("volume", 0),
            "url":       f"https://kalshi.com/markets/{ticker}",
        }

    def find_matching_market(self, question: str, kalshi_markets: list) -> dict | None:
        best_score = 0.0
        best = None

        for m in kalshi_markets:
            score = self._name_similarity(question, m.get("title", ""))
            if score > best_score:
                best_score = score
                best = m

        if best_score >= 0.5:
            return best
        return None

    @staticmethod
    def _name_similarity(a: str, b: str) -> float:
        a_words = set(a.lower().split())
        b_words = set(b.lower().split())
        stopwords = {"will", "the", "a", "an", "in", "of", "to", "be", "by",
                     "or", "and", "for", "on", "at", "is", "are", "was", "win"}
        a_words -= stopwords
        b_words -= stopwords
        if not a_words or not b_words:
            return 0.0
        intersection = a_words & b_words
        union = a_words | b_words
        jaccard = len(intersection) / len(union)
        contains_bonus = 0.3 if (a.lower() in b.lower() or b.lower() in a.lower()) else 0.0
        return min(1.0, jaccard + contains_bonus)
