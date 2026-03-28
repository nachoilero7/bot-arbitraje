"""
Signal: Odds Divergence (H2H)
Compara precios de Polymarket en mercados de partidos individuales contra
el consenso de bookmakers (Betfair, Pinnacle, William Hill, etc.).

Free tier de The Odds API incluye H2H para las 5 ligas principales.

Logica:
  1. Fetch H2H odds de bookmakers para cada liga (cacheado 1h)
  2. Para cada Polymarket market que mencione dos equipos + resultado,
     buscar el evento correspondiente por nombre de equipo + fecha
  3. Calcular probabilidad implicita del bookmaker (sin vig) para ese outcome
  4. Si poly_price < book_prob - fee => hay edge confirmado

Patrones de questions que matchea:
  "Will Arsenal beat Chelsea?" -> home_win o away_win de Arsenal
  "Will Real Madrid win vs Barcelona on March 20?" -> home_win de Real Madrid
  "Will the match end in a draw?" -> draw
"""
import re
import json
from datetime import datetime, timezone
from dataclasses import dataclass

from src.signals.base import BaseSignal, Opportunity, SignalType
from src.enrichers.odds_api import OddsApiClient
from src.utils.logger import get_logger

logger = get_logger(__name__)

SPORT_MAP = {
    "serie a":    "soccer_italy_serie_a",
    "bundesliga": "soccer_germany_bundesliga",
    "ligue 1":    "soccer_france_ligue_one",
    "la liga":    "soccer_spain_la_liga",
    "premier":    "soccer_epl",
    "nba":        "basketball_nba",
}

CACHE_TTL_SECS = 8 * 3600  # 8 horas — free tier: 500 req/mes, 5 sports × 3 veces/dia = 450/mes


@dataclass
class EnrichedOpportunity(Opportunity):
    bookmaker_prob: float = 0.0
    num_books: int = 0
    home_team: str = ""
    away_team: str = ""


class OddsDivergenceSignal(BaseSignal):

    def __init__(self, odds_client: OddsApiClient, fee_rate: float = 0.02, min_edge: float = 0.03):
        super().__init__(fee_rate=fee_rate, min_edge=min_edge)
        self.odds_client = odds_client
        self._h2h_cache: dict[str, list] = {}
        self._cache_time: dict[str, float] = {}

    @property
    def name(self) -> str:
        return "ODDS_DIVERGENCE"

    def _get_h2h(self, sport_key: str) -> list[dict]:
        import time
        now = time.time()
        if sport_key in self._h2h_cache and (now - self._cache_time.get(sport_key, 0)) < CACHE_TTL_SECS:
            return self._h2h_cache[sport_key]
        try:
            data = self.odds_client.get_h2h_odds(sport_key)
            self._h2h_cache[sport_key] = data
            self._cache_time[sport_key] = now
            logger.info(f"OddsAPI H2H cached: {sport_key} ({len(data)} events) | {self.odds_client.quota_status()}")
            return data
        except Exception as e:
            logger.warning(f"OddsAPI H2H failed for {sport_key}: {e}")
            return []

    def detect(self, markets: list[dict], prices: dict = None) -> list[Opportunity]:
        opportunities = []

        for market in markets:
            try:
                question = market.get("question", "")
                category = ""
                events = market.get("events")
                if events and isinstance(events, list) and events:
                    category = events[0].get("category", "") or ""

                # Detectar sport y si es mercado H2H
                sport_key = self._detect_sport(category, question)
                if not sport_key:
                    continue

                # Extraer equipos y outcome de la pregunta
                parsed = self._parse_h2h_question(question)
                if not parsed:
                    continue
                team_query, outcome_side = parsed  # ("Arsenal", "win") o ("", "draw")

                # Obtener precios del mercado
                outcome_prices_raw = market.get("outcomePrices")
                if not outcome_prices_raw:
                    continue
                outcome_prices = json.loads(outcome_prices_raw) if isinstance(outcome_prices_raw, str) else outcome_prices_raw
                if len(outcome_prices) < 2:
                    continue
                yes_price = float(outcome_prices[0])
                if yes_price <= 0 or yes_price >= 0.98:
                    continue

                # Buscar el evento correspondiente en bookmakers
                h2h_events = self._get_h2h(sport_key)
                match = self._find_event(h2h_events, team_query)
                if not match:
                    continue

                event, matched_team, role = match  # role: "home" o "away"
                book_prob = self._get_win_prob(event, role)
                if book_prob <= 0:
                    continue

                confirmed_edge = book_prob - yes_price - self.fee_rate
                if confirmed_edge < self.min_edge:
                    continue

                token_ids = json.loads(market.get("clobTokenIds", "[]")) if isinstance(market.get("clobTokenIds"), str) else []
                n_books = len(event.get("bookmakers", []))

                opp = EnrichedOpportunity(
                    signal_type=SignalType.MISPRICED_CORR,
                    condition_id=market.get("conditionId", ""),
                    question=question,
                    category=category,
                    token_id=token_ids[0] if token_ids else "",
                    side="YES",
                    market_price=yes_price,
                    fair_value=book_prob,
                    edge=round(confirmed_edge, 5),
                    edge_pct=round(confirmed_edge / yes_price, 4) if yes_price > 0 else 0,
                    best_bid=market.get("bestBid") or 0,
                    best_ask=market.get("bestAsk") or 0,
                    spread=market.get("spread") or 0,
                    liquidity_usd=market.get("liquidityNum") or 0,
                    volume_24h=market.get("volume24hr") or 0,
                    bookmaker_prob=book_prob,
                    num_books=n_books,
                    home_team=event.get("home_team", ""),
                    away_team=event.get("away_team", ""),
                    notes=(
                        f"Poly={yes_price:.3f} | Books({n_books})={book_prob:.3f} | "
                        f"edge={confirmed_edge:.4f} | {event.get('home_team')} vs {event.get('away_team')}"
                    )
                )
                opportunities.append(opp)
                logger.info(
                    f"[DIVERGENCE] {question[:50]} | "
                    f"poly={yes_price:.3f} books={book_prob:.3f} edge={confirmed_edge:.4f}"
                )

            except Exception as e:
                logger.debug(f"OddsDivergence skip: {e}")

        opportunities.sort(key=lambda o: o.edge, reverse=True)
        return opportunities

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _detect_sport(self, category: str, question: str) -> str | None:
        text = f"{category} {question}".lower()
        for keyword, sport_key in SPORT_MAP.items():
            if keyword in text:
                return sport_key
        return None

    def _parse_h2h_question(self, question: str) -> tuple[str, str] | None:
        """
        Extrae el equipo y el tipo de outcome de la pregunta.
        Retorna (team_name, "win") o (team_name, "draw") o None si no es H2H.

        Patrones:
          "Will Arsenal beat/defeat Chelsea?"         -> ("Arsenal", "win")
          "Will Arsenal win against/vs Chelsea?"      -> ("Arsenal", "win")
          "Will the Arsenal vs Chelsea match end in a draw?" -> ("", "draw")
          "Will Real Madrid win their match on March 20?" -> ("Real Madrid", "win")
        """
        q = question.strip()

        # Draw
        if re.search(r"\bdraw\b", q, re.I):
            return ("", "draw")

        # "Will TEAM beat/defeat/win..."
        m = re.match(
            r"Will\s+(.+?)\s+(?:beat|defeat|win\s+(?:against|vs\.?|their)?|top)\s",
            q, re.I
        )
        if m:
            team = m.group(1).strip()
            # Filtrar si el team parece ser el resultado de una pregunta de futures
            # (contiene palabras como "finish", "be relegated", etc.)
            if not re.search(r"\b(finish|relegate|qualify|score|advance|top 4|champion)\b", team, re.I):
                return (team, "win")

        return None

    def _find_event(self, events: list[dict], team_query: str) -> tuple[dict, str, str] | None:
        """
        Busca el evento que contiene al equipo buscado.
        Retorna (event, matched_name, "home"/"away") o None.
        """
        if not team_query:
            return None

        best_score = 0
        best_match = None

        for event in events:
            home = event.get("home_team", "")
            away = event.get("away_team", "")

            for team, role in [(home, "home"), (away, "away")]:
                score = self._name_similarity(team_query, team)
                if score > best_score:
                    best_score = score
                    best_match = (event, team, role)

        # Umbral minimo de similaridad (evitar falsos positivos)
        if best_score >= 0.6:
            return best_match
        return None

    @staticmethod
    def _name_similarity(a: str, b: str) -> float:
        """Similaridad simple entre dos nombres de equipos."""
        a_words = set(a.lower().split())
        b_words = set(b.lower().split())
        stopwords = {"fc", "cf", "afc", "utd", "united", "city", "the", "de", "sc", "sv", "1.", "vfb", "vfl"}
        a_words -= stopwords
        b_words -= stopwords
        if not a_words or not b_words:
            return 0.0
        intersection = a_words & b_words
        union = a_words | b_words
        # Jaccard + bonus si uno contiene al otro
        jaccard = len(intersection) / len(union)
        contains_bonus = 0.3 if (a.lower() in b.lower() or b.lower() in a.lower()) else 0
        return min(1.0, jaccard + contains_bonus)

    def _get_win_prob(self, event: dict, role: str) -> float:
        """
        Calcula la probabilidad sin vig para que gane el equipo (home o away).
        Promedia sobre todos los bookmakers disponibles.
        """
        probs = []
        outcome_idx = 0 if role == "home" else 2  # 0=home, 1=draw, 2=away en H2H
        # Alternativa: buscar por nombre
        target_name = event.get("home_team" if role == "home" else "away_team", "")

        for book in event.get("bookmakers", []):
            for market in book.get("markets", []):
                outcomes = market.get("outcomes", [])
                if not outcomes:
                    continue
                # Buscar por nombre del equipo
                for i, o in enumerate(outcomes):
                    if self._name_similarity(o.get("name", ""), target_name) >= 0.5:
                        raw = [1.0 / max(x.get("price", 999), 0.01) for x in outcomes]
                        total = sum(raw)
                        if total > 0 and i < len(raw):
                            probs.append(raw[i] / total)
                        break

        return round(sum(probs) / len(probs), 5) if probs else 0.0
