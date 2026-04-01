"""
Enricher: Metaculus API
https://www.metaculus.com/api2 — requiere token desde 2024 (cuenta gratuita)

Para obtener el token:
  1. Registrarse en https://www.metaculus.com (gratis)
  2. Settings -> API -> copiar el token
  3. Agregar al .env: METACULUS_API_TOKEN=tu_token

Sin token: el cliente retorna None en todas las consultas (no rompe nada).
"""
import re
import time
import requests
from src.utils.logger import get_logger

logger = get_logger(__name__)

BASE_URL = "https://www.metaculus.com/api2"

CACHE_TTL_SECS = 2 * 3600  # 2 hours

# Stopwords to strip before building search query
STOPWORDS = {
    "will", "the", "be", "a", "an", "in", "on", "at", "to", "for", "of",
    "by", "is", "are", "was", "were", "has", "have", "had", "do", "does",
    "did", "would", "could", "should", "can", "may", "might", "shall",
    "it", "this", "that", "which", "who", "what", "when", "where", "how",
    "and", "or", "not", "no", "yes", "any", "all", "some", "from", "with",
    "as", "up",
}


def _tokenize(text: str) -> set[str]:
    """Lowercase, strip punctuation, return set of words."""
    return set(re.sub(r"[^\w\s]", "", text.lower()).split())


def _content_words(text: str) -> list[str]:
    """Extract content words (non-stopwords) from text."""
    return [w for w in _tokenize(text) if w and w not in STOPWORDS]


def _jaccard(set_a: set[str], set_b: set[str]) -> float:
    """Jaccard similarity between two word sets."""
    if not set_a or not set_b:
        return 0.0
    intersection = set_a & set_b
    union = set_a | set_b
    return len(intersection) / len(union)


class MetaculusClient:

    def __init__(self, api_token: str = None, proxy: str = None, timeout: int = 10):
        self.timeout   = timeout
        self._has_auth = bool(api_token)
        self.session   = requests.Session()
        if api_token:
            self.session.headers["Authorization"] = f"Token {api_token}"
        if proxy:
            self.session.proxies = {"http": proxy, "https": proxy}
        # Cache: { question[:80]: (probability, timestamp) }
        self._cache: dict[str, tuple[float, float]] = {}
        self._last_request_time: float = 0.0

    def _rate_limit(self) -> None:
        """Ensure at least 1.0s between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)

    def _get_questions(self, query: str) -> list[dict]:
        """Fetch questions from Metaculus search endpoint."""
        self._rate_limit()
        try:
            url = f"{BASE_URL}/questions/"
            params = {
                "search": query,
                "status": "open",
                "limit": 8,
                "type": "forecast",
            }
            resp = self.session.get(url, params=params, timeout=self.timeout)
            self._last_request_time = time.time()
            resp.raise_for_status()
            data = resp.json()
            return data.get("results", [])
        except Exception as e:
            logger.debug(f"MetaculusClient._get_questions failed: {e}")
            return []

    def find_probability(self, question: str) -> float | None:
        """
        Find Metaculus community prediction probability for a question.

        Steps:
          1. Extract top 5 content words from question
          2. Search Metaculus API
          3. Match best result by Jaccard similarity (threshold: 0.35)
          4. Return community_prediction.full.q2 (median) if found

        Returns float in [0, 1] or None if no confident match.
        """
        cache_key = question[:80]
        # Check cache
        if cache_key in self._cache:
            prob, ts = self._cache[cache_key]
            if time.time() - ts < CACHE_TTL_SECS:
                return prob

        # Sin token no podemos consultar la API (requiere auth desde 2024)
        if not self._has_auth:
            return None

        # Build search query from top 5 content words
        words = _content_words(question)
        search_query = " ".join(words[:5])
        if not search_query:
            return None

        results = self._get_questions(search_query)
        if not results:
            return None

        # Find best match by Jaccard similarity
        question_words = _tokenize(question)
        best_prob = None
        best_similarity = 0.0

        for result in results:
            title = result.get("title", "")
            if not title:
                continue
            title_words = _tokenize(title)
            similarity = _jaccard(question_words, title_words)

            if similarity > best_similarity:
                best_similarity = similarity
                # Extract median prediction (q2)
                cp = result.get("community_prediction") or {}
                full = cp.get("full") or {}
                q2 = full.get("q2")
                if q2 is not None:
                    best_prob = float(q2)
                else:
                    best_prob = None

        if best_similarity >= 0.35 and best_prob is not None:
            logger.debug(
                f"Metaculus match: sim={best_similarity:.3f} prob={best_prob:.3f} "
                f"for '{question[:60]}'"
            )
            self._cache[cache_key] = (best_prob, time.time())
            return best_prob

        return None
