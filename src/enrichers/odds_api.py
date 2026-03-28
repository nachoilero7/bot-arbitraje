"""
Enricher: The Odds API
https://the-odds-api.com — free tier: 500 requests/month

Obtiene odds de bookmakers (Betfair, Pinnacle, DraftKings, etc.) para eventos
deportivos y los convierte a probabilidades implícitas.

Soporta:
  - H2H (1x2 / moneyline): quien gana el partido
  - Outrights / Futures: quien gana la liga, clasificacion a top 4, descenso, etc.

Uso:
  client = OddsApiClient(api_key="TU_KEY")
  sports = client.get_sports()                         # lista de sports disponibles
  events = client.get_h2h_odds("soccer_epl")           # odds H2H de la Premier
  futures = client.get_outrights_odds("soccer_epl")    # futures: top scorer, relegation, etc.
"""
import time
import requests
from dataclasses import dataclass, field
from src.utils.logger import get_logger

logger = get_logger(__name__)

BASE_URL = "https://api.the-odds-api.com/v4"

# Bookmakers de referencia ordenados por agudeza (sharp primero)
SHARP_BOOKS = ["pinnacle", "betfair_ex_eu", "betfair", "matchbook"]
SOFT_BOOKS  = ["draftkings", "fanduel", "williamhill_us", "unibet"]


@dataclass
class BookmakerOdds:
    """Odds de un bookmaker para un outcome especifico."""
    bookmaker: str
    outcome: str
    decimal_odds: float
    implied_prob: float   # 1 / decimal_odds (sin vig)


@dataclass
class MarketOdds:
    """Consensus de odds para un evento/mercado."""
    event_id: str
    sport: str
    home_team: str
    away_team: str
    commence_time: str
    market_key: str       # "h2h", "outrights", etc.
    outcome: str          # El outcome especifico (ej: "Arsenal", "Yes", "No")

    # Probabilidades calculadas
    sharp_prob: float     # Probabilidad segun bookmakers sharp (Pinnacle/Betfair)
    consensus_prob: float # Promedio de todos los bookmakers (sin vig)
    best_back_odds: float # Mejores odds disponibles para este outcome
    num_books: int        # Cuantos bookmakers tienen odds

    raw_books: list[BookmakerOdds] = field(default_factory=list)


class OddsApiClient:

    # Mapeo de sports de Polymarket a sport_key de The Odds API
    SPORT_MAP = {
        "serie a":     "soccer_italy_serie_a",
        "bundesliga":  "soccer_germany_bundesliga",
        "ligue 1":     "soccer_france_ligue_one",
        "la liga":     "soccer_spain_la_liga",
        "premier":     "soccer_epl",
        "champions":   "soccer_uefa_champs_league",
        "nba":         "basketball_nba",
        "nfl":         "americanfootball_nfl",
        "mlb":         "baseball_mlb",
        "nhl":         "icehockey_nhl",
    }

    def __init__(self, api_key: str, timeout: int = 10):
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()
        self._requests_remaining = None
        self._requests_used = None

    def _get(self, path: str, params: dict = None) -> dict | list:
        params = params or {}
        params["apiKey"] = self.api_key
        url = f"{BASE_URL}{path}"
        try:
            resp = self.session.get(url, params=params, timeout=self.timeout)
            # Tracking de quota
            self._requests_remaining = resp.headers.get("x-requests-remaining")
            self._requests_used      = resp.headers.get("x-requests-used")
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.error(f"OddsAPI error: {url} — {e}")
            raise

    def quota_status(self) -> str:
        if self._requests_remaining is not None:
            return f"OddsAPI: {self._requests_used} used / {int(self._requests_used or 0) + int(self._requests_remaining or 0)} total"
        return "OddsAPI: quota unknown"

    def get_sports(self) -> list[dict]:
        """Lista todos los sports disponibles (1 request, no cuenta para quota activa)."""
        return self._get("/sports")

    def get_h2h_odds(self, sport_key: str, regions: str = "eu,uk") -> list[dict]:
        """
        Obtiene odds H2H (1x2) para todos los eventos activos de un sport.
        regions: eu (Betfair/Pinnacle), uk (William Hill), us (DraftKings)
        """
        return self._get(f"/sports/{sport_key}/odds", params={
            "regions": regions,
            "markets": "h2h",
            "oddsFormat": "decimal",
        })

    def get_outrights_odds(self, sport_key: str, regions: str = "eu,uk") -> list[dict]:
        """
        Obtiene odds de futures/outrights.
        NOTA: Requiere plan pagado en The Odds API. En free tier retorna [].
        """
        try:
            return self._get(f"/sports/{sport_key}/odds", params={
                "regions": regions,
                "markets": "outrights",
                "oddsFormat": "decimal",
            })
        except Exception as e:
            if "422" in str(e):
                logger.debug(f"Outrights not available on free tier for {sport_key}")
            else:
                logger.warning(f"Outrights failed for {sport_key}: {e}")
            return []

    # ── Conversion y consensus ────────────────────────────────────────────────

    @staticmethod
    def decimal_to_prob(decimal_odds: float) -> float:
        """Convierte odds decimales a probabilidad implicita (sin remover vig)."""
        if decimal_odds <= 0:
            return 0.0
        return round(1.0 / decimal_odds, 5)

    @staticmethod
    def remove_vig(raw_probs: list[float]) -> list[float]:
        """
        Remueve el vig (margen del bookmaker) normalizando las probabilidades.
        raw_probs: lista de probabilidades implicitas (suman > 1.0 por el vig)
        Retorna lista normalizada que suma a 1.0.
        """
        total = sum(raw_probs)
        if total <= 0:
            return raw_probs
        return [p / total for p in raw_probs]

    def extract_consensus(self, event: dict, outcome_name: str) -> MarketOdds | None:
        """
        Extrae probabilidad de consenso para un outcome especifico de un evento.
        Prioriza bookmakers sharp (Pinnacle, Betfair).
        """
        bookmakers = event.get("bookmakers", [])
        if not bookmakers:
            return None

        all_books: list[BookmakerOdds] = []

        for book in bookmakers:
            book_key = book.get("key", "")
            for market in book.get("markets", []):
                outcomes = market.get("outcomes", [])
                # Normalizar todos los outcomes del market primero (remover vig)
                raw_probs = [self.decimal_to_prob(o.get("price", 0)) for o in outcomes]
                norm_probs = self.remove_vig(raw_probs)

                for i, o in enumerate(outcomes):
                    if self._match_outcome(o.get("name", ""), outcome_name):
                        all_books.append(BookmakerOdds(
                            bookmaker=book_key,
                            outcome=o.get("name", ""),
                            decimal_odds=o.get("price", 0),
                            implied_prob=norm_probs[i] if i < len(norm_probs) else 0,
                        ))

        if not all_books:
            return None

        # Probabilidad sharp (solo bookmakers agudos)
        sharp = [b for b in all_books if b.bookmaker in SHARP_BOOKS]
        sharp_prob = sum(b.implied_prob for b in sharp) / len(sharp) if sharp else 0

        # Consensus general
        consensus_prob = sum(b.implied_prob for b in all_books) / len(all_books)

        # Mejores odds disponibles para este outcome
        best_odds = max((b.decimal_odds for b in all_books), default=0)

        return MarketOdds(
            event_id=event.get("id", ""),
            sport=event.get("sport_key", ""),
            home_team=event.get("home_team", ""),
            away_team=event.get("away_team", ""),
            commence_time=event.get("commence_time", ""),
            market_key="h2h",
            outcome=outcome_name,
            sharp_prob=round(sharp_prob, 5),
            consensus_prob=round(consensus_prob, 5),
            best_back_odds=best_odds,
            num_books=len(all_books),
            raw_books=all_books,
        )

    @staticmethod
    def _match_outcome(book_name: str, target: str) -> bool:
        """Matching flexible de nombres de outcomes (case-insensitive, partial)."""
        bn = book_name.lower().strip()
        tn = target.lower().strip()
        return bn == tn or tn in bn or bn in tn
