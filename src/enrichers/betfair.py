"""
Enricher: Betfair Exchange API
https://developer.betfair.com — gratis con cuenta + App Key de Delayed Data

Betfair es un EXCHANGE (no bookmaker) — los precios los fija el mercado.
Ventajas vs Pinnacle:
  - Sin vig del bookmaker (solo 5% comision sobre ganancias)
  - Precios ultra-sharp (consenso real del mercado)
  - App Key gratuita con cuenta registrada (no requiere deposito)
  - Cubre outrights Y H2H para NBA, CL, ligas europeas, NFL, etc.

Setup (10 minutos):
  1. Registrarse en https://www.betfair.com/exchange/plus/
  2. Ir a https://developer.betfair.com/devtools/my-api/ → "Create app key"
  3. Elegir "Delayed data" (gratis, datos con ~2 min delay)
  4. Copiar la App Key al .env como BETFAIR_APP_KEY
  5. Agregar BETFAIR_USERNAME y BETFAIR_PASSWORD del registro

API flow:
  POST /api/login → obtiene sessionToken (valido 4hs)
  POST exchange/betting/rest/v1/listMarketCatalogue → lista mercados
  POST exchange/betting/rest/v1/listMarketBook → precios de cada mercado

Betfair market types para outrights:
  WINNER         → Ganador de liga / campeon de torneo
  NEXT_MANAGER   → Proximo manager (para outrights de club)
  TOP_4_FINISH   → Clasificacion top 4

Betfair event type IDs:
  Soccer=1, Basketball=7522, AmericanFootball=6423, Baseball=7511, IceHockey=7524
"""
import time
import requests
from dataclasses import dataclass
from src.utils.logger import get_logger

logger = get_logger(__name__)

LOGIN_URL  = "https://identitysso.betfair.com/api/login"
API_URL    = "https://api.betfair.com/exchange/betting/rest/v1/"
CONTENT_TYPE = "application/x-www-form-urlencoded"

# Betfair Event Type IDs
EVENT_TYPES = {
    "basketball_nba":           "7522",
    "americanfootball_nfl":     "6423",
    "soccer_epl":               "1",
    "soccer_spain_la_liga":     "1",
    "soccer_germany_bundesliga": "1",
    "soccer_italy_serie_a":     "1",
    "soccer_france_ligue_one":  "1",
    "soccer_uefa_champs_league": "1",
    "baseball_mlb":             "7511",
    "icehockey_nhl":            "7524",
}

# Betfair Competition IDs para filtrar ligas especificas
COMPETITION_IDS = {
    "basketball_nba":            "10328599",  # NBA
    "soccer_epl":                "10932509",  # Premier League
    "soccer_spain_la_liga":      "117",        # La Liga
    "soccer_germany_bundesliga": "59",         # Bundesliga
    "soccer_italy_serie_a":      "81",         # Serie A
    "soccer_france_ligue_one":   "55",         # Ligue 1
    "soccer_uefa_champs_league": "228",        # Champions League
}

SESSION_TTL = 3 * 3600  # renovar session cada 3 horas (dura 4)
CACHE_TTL   = 900        # 15 minutos para outrights


@dataclass
class BetfairOutright:
    sport_key: str
    market_id: str
    market_name: str        # ej: "UEFA Champions League Winner 2024/25"
    runner_name: str        # ej: "Real Madrid"
    back_price: float       # mejor precio disponible para apostar a favor
    no_vig_prob: float      # probabilidad estimada sin comision
    total_matched: float    # volumen total apostado en este mercado


class BetfairClient:

    def __init__(self, username: str, password: str, app_key: str, timeout: int = 10):
        self.username = username
        self.password = password
        self.app_key  = app_key
        self.timeout  = timeout
        self.session  = requests.Session()
        self._session_token: str | None = None
        self._session_time: float = 0
        self._cache: dict[str, tuple[float, list]] = {}

    # ── Auth ──────────────────────────────────────────────────────────────────

    def _ensure_session(self) -> bool:
        """Obtiene o renueva el session token de Betfair."""
        if self._session_token and (time.time() - self._session_time) < SESSION_TTL:
            return True
        try:
            resp = self.session.post(LOGIN_URL, data={
                "username": self.username,
                "password": self.password,
            }, headers={
                "X-Application": self.app_key,
                "Accept": "application/json",
                "Content-Type": CONTENT_TYPE,
            }, timeout=self.timeout)
            data = resp.json()
            if data.get("status") == "SUCCESS":
                self._session_token = data["token"]
                self._session_time  = time.time()
                logger.info("Betfair session renewed")
                return True
            logger.warning(f"Betfair login failed: {data.get('error', data)}")
            return False
        except Exception as e:
            logger.warning(f"Betfair login error: {e}")
            return False

    def _post(self, endpoint: str, body: dict) -> dict | list | None:
        if not self._ensure_session():
            return None
        try:
            resp = self.session.post(
                API_URL + endpoint,
                json=body,
                headers={
                    "X-Application":   self.app_key,
                    "X-Authentication": self._session_token,
                    "Accept":           "application/json",
                    "Content-Type":     "application/json",
                },
                timeout=self.timeout,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Betfair API error: {endpoint} — {e}")
            return None

    # ── Outrights ─────────────────────────────────────────────────────────────

    def get_outrights(self, sport_key: str) -> list[dict]:
        """
        Obtiene mercados de outright (WINNER) para un sport.
        Retorna lista de mercados con sus runners y precios.
        """
        cache_key = f"outrights_{sport_key}"
        now = time.time()
        if cache_key in self._cache:
            ts, data = self._cache[cache_key]
            if now - ts < CACHE_TTL:
                return data

        event_type_id = EVENT_TYPES.get(sport_key)
        if not event_type_id:
            return []

        # Paso 1: listar mercados de tipo WINNER
        filter_params = {
            "eventTypeIds":  [event_type_id],
            "marketTypeCodes": ["WINNER", "NEXT_MANAGER"],
            "inPlayOnly":    False,
        }
        competition_id = COMPETITION_IDS.get(sport_key)
        if competition_id:
            filter_params["competitionIds"] = [competition_id]

        catalogue = self._post("en/listMarketCatalogue/", {
            "filter":         filter_params,
            "marketProjection": ["RUNNER_DESCRIPTION", "EVENT", "COMPETITION"],
            "maxResults":     50,
            "sort":           "FIRST_TO_START",
        })
        if not catalogue:
            return []

        # Paso 2: obtener precios de todos esos mercados
        market_ids = [m["marketId"] for m in catalogue]
        if not market_ids:
            return []

        books = self._post("en/listMarketBook/", {
            "marketIds": market_ids,
            "priceProjection": {
                "priceData": ["EX_BEST_OFFERS"],
                "exBestOffersOverrides": {"bestPricesDepth": 1},
            },
        })
        if not books:
            return []

        # Combinar catalogue + books
        books_by_id = {b["marketId"]: b for b in books}
        result = []
        for market in catalogue:
            mid = market["marketId"]
            book = books_by_id.get(mid, {})
            result.append({
                "marketId":   mid,
                "marketName": market.get("marketName", ""),
                "event":      market.get("event", {}),
                "competition": market.get("competition", {}),
                "runners":    market.get("runners", []),
                "runners_book": {r["selectionId"]: r for r in book.get("runners", [])},
                "status":     book.get("status", ""),
            })

        self._cache[cache_key] = (now, result)
        logger.info(f"Betfair outrights cached: {sport_key} ({len(result)} markets)")
        return result

    def extract_outright_probs(self, markets: list[dict]) -> list[BetfairOutright]:
        """
        Convierte mercados de Betfair en BetfairOutright con probabilidades sin vig.
        """
        results = []
        for market in markets:
            runners     = market.get("runners", [])
            runners_book = market.get("runners_book", {})
            market_name = market.get("marketName", "")

            # Recopilar mejor back price de cada runner
            runner_prices = []
            for runner in runners:
                sid   = runner["selectionId"]
                name  = runner.get("runnerName", "")
                book  = runners_book.get(sid, {})
                ex    = book.get("ex", {})
                backs = ex.get("availableToBack", [])
                price = backs[0]["price"] if backs else 0
                if price > 1.0:
                    runner_prices.append((name, price))

            if not runner_prices:
                continue

            # Calcular probs sin vig
            raw_probs = [1.0 / p for _, p in runner_prices]
            total     = sum(raw_probs)
            if total <= 0:
                continue

            for (name, price), raw_prob in zip(runner_prices, raw_probs):
                no_vig_prob = raw_prob / total
                results.append(BetfairOutright(
                    sport_key="",
                    market_id=market["marketId"],
                    market_name=market_name,
                    runner_name=name,
                    back_price=price,
                    no_vig_prob=round(no_vig_prob, 5),
                    total_matched=0,
                ))

        return results

    def get_outright_prob(
        self,
        markets: list[dict],
        team_name: str,
        market_keyword: str = "",
    ) -> "BetfairOutright | None":
        """Busca la probabilidad de un equipo/jugador en los outrights de Betfair."""
        probs = self.extract_outright_probs(markets)

        best_score = 0.0
        best = None
        for p in probs:
            if market_keyword and market_keyword.lower() not in p.market_name.lower():
                continue
            score = self._name_similarity(team_name, p.runner_name)
            if score > best_score:
                best_score = score
                best = p

        return best if best_score >= 0.5 else None

    @staticmethod
    def _name_similarity(a: str, b: str) -> float:
        a_words = set(a.lower().split())
        b_words = set(b.lower().split())
        stopwords = {"fc", "cf", "afc", "utd", "united", "city", "the", "de",
                     "sc", "sv", "1.", "vfb", "vfl", "real", "atletico"}
        a_words -= stopwords
        b_words -= stopwords
        if not a_words or not b_words:
            return 0.0
        intersection = a_words & b_words
        union        = a_words | b_words
        jaccard      = len(intersection) / len(union)
        contains     = 0.3 if (a.lower() in b.lower() or b.lower() in a.lower()) else 0.0
        return min(1.0, jaccard + contains)
