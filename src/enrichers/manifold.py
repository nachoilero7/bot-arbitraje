"""
Enricher: Manifold Markets API
https://manifold.markets — prediction market platform, free, no auth required.

Busca precios de probabilidad en Manifold para enriquecer senales de Polymarket:
  Polymarket YES = 0.45, Manifold YES = 0.58 → posible edge

Endpoint:
  GET https://manifold.markets/api/v0/search-markets?term={query}&limit=5
  Response: [{"question": "...", "probability": 0.73, "isResolved": false, "closeTime": 12345678}]
"""
import time
import requests
from src.utils.logger import get_logger

logger = get_logger(__name__)

BASE_URL = "https://manifold.markets/api/v0"
CACHE_TTL_SECS = 7200   # 2 hours
RATE_LIMIT_SECS = 1.0   # 1 req/sec

_STOP_WORDS = {
    "will", "the", "be", "a", "an", "in", "on", "at", "to", "for", "of",
    "by", "is", "are", "was", "were", "has", "have", "had", "do", "does",
    "did", "would", "could", "should", "can", "may", "might", "shall",
    "it", "this", "that", "which", "who", "what", "when", "where", "how",
    "and", "or", "not", "no", "yes", "any", "all", "some", "from", "with",
    "as", "up",
}

JACCARD_THRESHOLD = 0.35


def _extract_words(text: str) -> set[str]:
    """Lowercase, split, remove stop words and short tokens."""
    words = set(text.lower().split())
    return {w.strip("?.,!:;\"'()[]") for w in words} - _STOP_WORDS - {""}


def _top_words(text: str, n: int = 5) -> list[str]:
    """Return up to n content words, preserving insertion order."""
    seen: set[str] = set()
    result: list[str] = []
    for word in text.lower().split():
        clean = word.strip("?.,!:;\"'()[]")
        if clean and clean not in _STOP_WORDS and clean not in seen:
            seen.add(clean)
            result.append(clean)
            if len(result) == n:
                break
    return result


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


class ManifoldClient:
    """
    Client for Manifold Markets search API.

    Args:
        proxy: optional proxy URL passed to requests (e.g. "http://proxy:8080")
    """

    def __init__(self, proxy: str | None = None):
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        if proxy:
            self.session.proxies.update({"http": proxy, "https": proxy})

        self._cache: dict[str, tuple[float, float | None]] = {}
        self._last_request_time: float = 0.0

    def _throttle(self) -> None:
        elapsed = time.time() - self._last_request_time
        if elapsed < RATE_LIMIT_SECS:
            time.sleep(RATE_LIMIT_SECS - elapsed)

    def _fetch(self, query: str) -> list[dict]:
        self._throttle()
        url = f"{BASE_URL}/search-markets"
        params = {"term": query, "limit": 5}
        try:
            resp = self.session.get(url, params=params, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.warning(f"Manifold API error for query '{query}': {e}")
            return []
        finally:
            self._last_request_time = time.time()

    def find_probability(self, question: str) -> float | None:
        """
        Search Manifold for a market matching `question` and return its probability.

        Uses Jaccard similarity on content words. Returns None if no good match
        is found (similarity < JACCARD_THRESHOLD) or the best match is resolved.

        Results are cached for 2 hours per question.
        """
        cache_key = question.strip().lower()
        now = time.time()

        if cache_key in self._cache:
            ts, cached_value = self._cache[cache_key]
            if now - ts < CACHE_TTL_SECS:
                return cached_value

        try:
            top_words = _top_words(question, n=5)
            if not top_words:
                return None

            query = " ".join(top_words)
            results = self._fetch(query)

            question_words = _extract_words(question)
            best_similarity = 0.0
            best_result: dict | None = None

            for result in results:
                if result.get("isResolved", True):
                    continue  # solo mercados activos
                if result.get("outcomeType") != "BINARY":
                    continue  # solo binarios tienen campo probability
                prob_raw = result.get("probability")
                if prob_raw is None:
                    continue
                result_words = _extract_words(result.get("question", ""))
                sim = _jaccard(question_words, result_words)
                if sim > best_similarity:
                    best_similarity = sim
                    best_result = result

            probability: float | None = None
            if best_result is not None and best_similarity >= JACCARD_THRESHOLD:
                probability = float(best_result.get("probability"))
                logger.debug(
                    f"Manifold match (sim={best_similarity:.2f}): "
                    f"'{best_result.get('question', '')[:60]}' → {probability:.3f}"
                )
            else:
                logger.debug(
                    f"Manifold no match for '{question[:60]}' "
                    f"(best_sim={best_similarity:.2f})"
                )

            self._cache[cache_key] = (now, probability)
            return probability

        except Exception as e:
            logger.warning(f"ManifoldClient.find_probability error: {e}")
            return None
