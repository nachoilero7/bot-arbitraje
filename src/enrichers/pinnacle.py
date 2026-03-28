"""
Enricher: Pinnacle Sports API
https://api.pinnacle.com — gratis con cuenta activa (requiere deposito minimo)

Obtiene odds de Pinnacle (el bookmaker mas sharp del mercado) para:
  - Outrights / Futures: ganador de liga, campeon, clasificacion
  - Sports: soccer, basketball, americanfootball, baseball, icehockey

Pinnacle es el mejor referente porque:
  - No limita cuentas ganadoras (todos los bookmakers calibran vs Pinnacle)
  - Vig muy bajo (~2% vs 5-10% de soft books)
  - Sus precios son el "precio justo" del mercado

Auth: Basic Auth (username:password en Base64)

Endpoints clave:
  GET /v1/sports                          → lista de sports
  GET /v1/leagues?sportId=29              → ligas (soccer=29, basketball=4, etc.)
  GET /v1/odds/special?sportId=4          → outrights/futures (NBA champion, etc.)
  GET /v1/odds?sportId=29&leagueIds=1980  → H2H de partidos individuales

Mapeo sportId Pinnacle:
  Soccer=29, Basketball=4, Football=15, Baseball=3, Hockey=19, Tennis=33

Uso:
  client = PinnacleApiClient(username="tu_user", password="tu_pass")
  specials = client.get_specials(sport_id=4)   # NBA outrights
  prob = client.get_outright_prob(specials, "Oklahoma City Thunder", "NBA Championship")
"""
import base64
import time
import requests
from dataclasses import dataclass, field
from src.utils.logger import get_logger

logger = get_logger(__name__)

BASE_URL = "https://api.pinnacle.com"

# sportId de Pinnacle
SPORT_IDS = {
    "basketball_nba":           4,
    "americanfootball_nfl":     15,
    "soccer_epl":               29,
    "soccer_spain_la_liga":     29,
    "soccer_germany_bundesliga": 29,
    "soccer_italy_serie_a":     29,
    "soccer_france_ligue_one":  29,
    "soccer_uefa_champs_league": 29,
    "baseball_mlb":             3,
    "icehockey_nhl":            19,
    "tennis_atp":               33,
}

# leagueId de Pinnacle para los outrights mas relevantes para Polymarket
LEAGUE_IDS = {
    "basketball_nba":            487,   # NBA
    "soccer_epl":                1980,  # Premier League
    "soccer_spain_la_liga":      2537,  # La Liga
    "soccer_germany_bundesliga": 1452,  # Bundesliga
    "soccer_italy_serie_a":      1368,  # Serie A
    "soccer_france_ligue_one":   2036,  # Ligue 1
    "soccer_uefa_champs_league": 2627,  # Champions League
    "americanfootball_nfl":      889,   # NFL
    "baseball_mlb":              246,   # MLB
    "icehockey_nhl":             1054,  # NHL
}

CACHE_TTL_SECS = 900  # 15 minutos para outrights (cambian menos seguido)


@dataclass
class PinnacleOutright:
    """Probabilidad de Pinnacle para un outcome de outright."""
    sport_key: str
    market_name: str        # ej: "NBA Championship 2024/2025"
    contestant_name: str    # ej: "Oklahoma City Thunder"
    decimal_odds: float
    no_vig_prob: float      # probabilidad sin vig, ya normalizada
    num_contestants: int    # cuantos participantes en el mercado


class PinnacleApiClient:

    def __init__(self, username: str, password: str, timeout: int = 10):
        self.timeout = timeout
        self.session = requests.Session()
        # Basic Auth
        creds = base64.b64encode(f"{username}:{password}".encode()).decode()
        self.session.headers.update({
            "Authorization": f"Basic {creds}",
            "Accept": "application/json",
        })
        self._cache: dict[str, tuple[float, list]] = {}  # key -> (timestamp, data)

    def _get(self, path: str, params: dict = None) -> dict | list:
        url = f"{BASE_URL}{path}"
        try:
            resp = self.session.get(url, params=params or {}, timeout=self.timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.error(f"Pinnacle API error: {url} — {e}")
            raise

    def _cached_get(self, cache_key: str, path: str, params: dict = None) -> list:
        now = time.time()
        if cache_key in self._cache:
            ts, data = self._cache[cache_key]
            if now - ts < CACHE_TTL_SECS:
                return data
        try:
            data = self._get(path, params)
            # La respuesta puede ser dict con lista o lista directa
            if isinstance(data, dict):
                # Pinnacle a veces envuelve en {"specials": [...]} o similar
                for key in ("specials", "leagues", "sports", "events"):
                    if key in data:
                        data = data[key]
                        break
            if not isinstance(data, list):
                data = []
            self._cache[cache_key] = (now, data)
            logger.info(f"Pinnacle cached: {cache_key} ({len(data)} entries)")
            return data
        except Exception as e:
            logger.warning(f"Pinnacle fetch failed for {cache_key}: {e}")
            return []

    def get_sports(self) -> list[dict]:
        """Lista todos los sports activos."""
        return self._get("/v1/sports")

    def get_specials(self, sport_id: int) -> list[dict]:
        """
        Obtiene outrights/futures (specials) para un sport.
        Incluye: NBA Championship, CL Winner, Liga winner, etc.
        """
        cache_key = f"specials_{sport_id}"
        return self._cached_get(
            cache_key,
            "/v1/odds/special",
            params={"sportId": sport_id, "oddsFormat": "Decimal"},
        )

    def get_specials_for_sport_key(self, sport_key: str) -> list[dict]:
        """Wrapper usando sport_key de The Odds API format."""
        sport_id = SPORT_IDS.get(sport_key)
        if not sport_id:
            return []
        return self.get_specials(sport_id)

    # ── Conversion ────────────────────────────────────────────────────────────

    @staticmethod
    def remove_vig(raw_probs: list[float]) -> list[float]:
        total = sum(raw_probs)
        if total <= 0:
            return raw_probs
        return [p / total for p in raw_probs]

    def extract_outright_probs(self, specials: list[dict]) -> list[PinnacleOutright]:
        """
        Convierte la respuesta raw de Pinnacle specials en PinnacleOutright objects.
        Cada special tiene: id, betType, category, contestants[{id, name, price}]
        """
        results = []
        for special in specials:
            if not isinstance(special, dict):
                continue
            market_name = special.get("category", "") or special.get("name", "") or ""
            contestants = special.get("contestants", [])
            if not contestants:
                # Pinnacle puede anidar en special.lines o special.periods
                for alt_key in ("lines", "periods", "prices"):
                    if alt_key in special and isinstance(special[alt_key], list):
                        contestants = special[alt_key]
                        break
            if not contestants:
                continue

            # Calcular probs sin vig
            prices = []
            names = []
            for c in contestants:
                if isinstance(c, dict):
                    price = c.get("price") or c.get("odds") or 0
                    name = c.get("name") or c.get("contestantName") or ""
                    if price > 1.0:  # odds decimales validos
                        prices.append(price)
                        names.append(name)

            if not prices:
                continue

            raw_probs = [1.0 / p for p in prices]
            no_vig_probs = self.remove_vig(raw_probs)

            for i, (name, prob) in enumerate(zip(names, no_vig_probs)):
                results.append(PinnacleOutright(
                    sport_key="",
                    market_name=market_name,
                    contestant_name=name,
                    decimal_odds=prices[i],
                    no_vig_prob=round(prob, 5),
                    num_contestants=len(prices),
                ))

        return results

    def get_outright_prob(
        self,
        specials: list[dict],
        team_name: str,
        market_keyword: str = "",
    ) -> "PinnacleOutright | None":
        """
        Busca la probabilidad de un equipo/jugador en los outrights.

        Args:
            specials: respuesta raw de get_specials()
            team_name: nombre del equipo a buscar (ej: "Oklahoma City Thunder")
            market_keyword: filtro opcional del mercado (ej: "Championship", "Winner")

        Returns:
            PinnacleOutright con la mejor coincidencia, o None
        """
        probs = self.extract_outright_probs(specials)

        best_score = 0.0
        best = None

        for p in probs:
            # Filtrar por tipo de mercado si se especifica
            if market_keyword and market_keyword.lower() not in p.market_name.lower():
                continue

            score = self._name_similarity(team_name, p.contestant_name)
            if score > best_score:
                best_score = score
                best = p

        if best_score >= 0.5:
            return best
        return None

    @staticmethod
    def _name_similarity(a: str, b: str) -> float:
        """Jaccard similarity entre nombres de equipos."""
        a_words = set(a.lower().split())
        b_words = set(b.lower().split())
        stopwords = {"fc", "cf", "afc", "utd", "united", "city", "the", "de", "sc",
                     "sv", "1.", "vfb", "vfl", "bv", "borussia", "real", "atletico"}
        a_words -= stopwords
        b_words -= stopwords
        if not a_words or not b_words:
            return 0.0
        intersection = a_words & b_words
        union = a_words | b_words
        jaccard = len(intersection) / len(union)
        contains_bonus = 0.3 if (a.lower() in b.lower() or b.lower() in a.lower()) else 0.0
        return min(1.0, jaccard + contains_bonus)
