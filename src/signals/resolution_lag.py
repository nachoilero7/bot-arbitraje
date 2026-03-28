"""
Signal: Resolution Lag
Detecta mercados donde el resultado ya es conocido pero Polymarket
no actualizó el precio todavía. Ventana de oportunidad: 5-15 minutos.

Ejemplo:
  Arsenal ganó 2-0, pero "Will Arsenal win?" sigue en 0.65
  → comprar YES a 0.65 (valor real: ~0.99)

Fuentes:
  - AllSportsAPI2 via RapidAPI (live scores — multideporte)
  - Requiere RAPIDAPI_KEY en .env
"""
import re
import json
import time
import requests
from dataclasses import dataclass

from src.signals.base import BaseSignal, Opportunity, SignalType
from src.utils.logger import get_logger

logger = get_logger(__name__)

LIVE_SCORES_URL = "https://allsportsapi2.p.rapidapi.com/api/matches/live"
RAPIDAPI_HOST   = "allsportsapi2.p.rapidapi.com"
CACHE_TTL_SECS  = 20 * 60  # 20 minutos — plan free: 100 req/dia, 3/hora = 72/dia

# Status codes de AllSportsAPI2 para football
STATUS_1ST_HALF = 6
STATUS_HALFTIME = 31
STATUS_2ND_HALF = 7
STATUS_INPROGRESS_TYPES = {"inprogress"}


@dataclass
class LiveMatch:
    home_team: str
    away_team: str
    home_goals: int
    away_goals: int
    minute: int
    sport: str
    status: str


class ResolutionLagSignal(BaseSignal):
    """
    Detecta oportunidades cuando el resultado de un partido ya es
    prácticamente seguro (ej: ganando 2-0 a los 85min) pero el precio
    en Polymarket no lo refleja todavía.
    """

    def __init__(self, rapidapi_key: str, fee_rate: float = 0.02, min_edge: float = 0.03):
        super().__init__(fee_rate=fee_rate, min_edge=min_edge)
        self.rapidapi_key = rapidapi_key
        self._live_cache: tuple[float, list[LiveMatch]] | None = None

    @property
    def name(self) -> str:
        return "RESOLUTION_LAG"

    def detect(self, markets: list[dict], prices: dict = None) -> list[Opportunity]:
        opportunities = []

        live_matches = self._get_live_scores()
        if not live_matches:
            return []

        for market in markets:
            try:
                question = market.get("question", "")
                if not question:
                    continue

                if not self._is_match_result_question(question):
                    continue

                teams = self._parse_team_names(question)
                if not teams:
                    continue

                match = self._find_live_match(live_matches, teams)
                if not match:
                    continue

                implied_prob = self._score_to_probability(match, teams)
                if implied_prob <= 0:
                    continue

                outcome_prices_raw = market.get("outcomePrices")
                if not outcome_prices_raw:
                    continue
                outcome_prices = json.loads(outcome_prices_raw) if isinstance(outcome_prices_raw, str) else outcome_prices_raw
                if len(outcome_prices) < 2:
                    continue

                yes_price = float(outcome_prices[0])
                # Excluir mercados ya actualizados (>0.85) o con precio irrisorio (<0.10)
                # Si ya está en 0.85+, el lag ya fue aprovechado por otros
                if yes_price <= 0.10 or yes_price >= 0.85:
                    continue

                edge = implied_prob - yes_price - self.fee_rate
                if edge < self.min_edge:
                    continue

                token_ids = json.loads(market.get("clobTokenIds", "[]")) if isinstance(market.get("clobTokenIds"), str) else []
                category = self._get_category(market)

                opp = Opportunity(
                    signal_type=SignalType.RESOLUTION_LAG,
                    condition_id=market.get("conditionId", ""),
                    question=question,
                    category=category,
                    token_id=token_ids[0] if token_ids else "",
                    side="YES",
                    market_price=yes_price,
                    fair_value=implied_prob,
                    edge=round(edge, 5),
                    edge_pct=round(edge / yes_price, 4) if yes_price > 0 else 0,
                    best_bid=market.get("bestBid") or 0,
                    best_ask=market.get("bestAsk") or 0,
                    spread=market.get("spread") or 0,
                    liquidity_usd=market.get("liquidityNum") or 0,
                    volume_24h=market.get("volume24hr") or 0,
                    notes=(
                        f"Poly={yes_price:.3f} | implied={implied_prob:.3f} | edge={edge:.4f} | "
                        f"{match.home_team} {match.home_goals}-{match.away_goals} "
                        f"{match.away_team} [{match.minute}']"
                    )
                )
                opportunities.append(opp)
                logger.info(
                    f"[RESOLUTION_LAG] {question[:50]} | "
                    f"poly={yes_price:.3f} implied={implied_prob:.3f} edge={edge:.4f}"
                )

            except Exception as e:
                logger.debug(f"ResolutionLag skip {market.get('conditionId','?')[:12]}: {e}")

        opportunities.sort(key=lambda o: o.edge, reverse=True)
        return opportunities

    # ── Live scores ────────────────────────────────────────────────────────────

    def _get_live_scores(self) -> list[LiveMatch]:
        now = time.time()
        if self._live_cache is not None:
            ts, data = self._live_cache
            if now - ts < CACHE_TTL_SECS:
                return data

        try:
            resp = requests.get(
                LIVE_SCORES_URL,
                headers={
                    "X-RapidAPI-Key": self.rapidapi_key,
                    "X-RapidAPI-Host": RAPIDAPI_HOST,
                },
                timeout=10,
            )
            resp.raise_for_status()
            raw = resp.json()
            matches = self._parse_events(raw.get("events", []))
            self._live_cache = (now, matches)
            logger.info(f"AllSportsAPI2: {len(matches)} live matches fetched")
            return matches
        except Exception as e:
            logger.warning(f"AllSportsAPI2 live scores failed: {e}")
            if self._live_cache is not None:
                _, stale = self._live_cache
                return stale
            return []

    def _parse_events(self, events: list) -> list[LiveMatch]:
        matches = []
        now = time.time()

        for event in events:
            try:
                status = event.get("status", {})
                if status.get("type") not in STATUS_INPROGRESS_TYPES:
                    continue

                status_code = status.get("code", 0)
                sport_slug = (
                    event.get("tournament", {})
                        .get("category", {})
                        .get("sport", {})
                        .get("slug", "")
                )

                home_team = event.get("homeTeam", {}).get("name", "")
                away_team = event.get("awayTeam", {}).get("name", "")
                if not home_team or not away_team:
                    continue

                home_goals = event.get("homeScore", {}).get("current", 0) or 0
                away_goals = event.get("awayScore", {}).get("current", 0) or 0

                # Calcular minuto de juego
                period_start = event.get("time", {}).get("currentPeriodStartTimestamp", 0)
                elapsed = int((now - period_start) / 60) if period_start else 0

                if status_code == STATUS_1ST_HALF:
                    minute = min(45, elapsed)
                elif status_code == STATUS_HALFTIME:
                    minute = 45
                elif status_code == STATUS_2ND_HALF:
                    minute = min(90, 45 + elapsed)
                else:
                    minute = elapsed

                matches.append(LiveMatch(
                    home_team=home_team,
                    away_team=away_team,
                    home_goals=int(home_goals),
                    away_goals=int(away_goals),
                    minute=minute,
                    sport=sport_slug,
                    status=status.get("description", ""),
                ))
            except Exception as e:
                logger.debug(f"Event parse skip: {e}")

        return matches

    # ── Probability model ──────────────────────────────────────────────────────

    def _score_to_probability(self, match: LiveMatch, queried_teams: list[str]) -> float:
        """
        Modelo de probabilidad conservador basado en investigación empírica.
        Solo señaliza cuando el resultado es casi matemáticamente seguro.

        Umbrales calibrados para minimizar falsos positivos:
          - Ventana de 5-10 min de lag documentada por papers académicos
          - A los 60 min, una ventaja de 2 goles tiene ~88% win prob (no 97%)
          - Solo señalizamos cuando la prob implícita > 0.90 con alta certeza
        """
        minute = match.minute
        home_goals = match.home_goals
        away_goals = match.away_goals
        goal_diff = home_goals - away_goals

        # Umbral de similitud más alto para evitar falsos matches
        home_sim = max(self._name_similarity(t, match.home_team) for t in queried_teams)
        away_sim = max(self._name_similarity(t, match.away_team) for t in queried_teams)

        if home_sim < 0.55 and away_sim < 0.55:
            return 0.0

        def _win_prob(diff: int, min_: int) -> float:
            # Solo condiciones donde la probabilidad real supera 0.90
            # basado en in-play win probability models (Dixon-Robinson, etc.)
            if diff >= 3 and min_ >= 55:
                return 0.97   # 3+ goles, >55 min: prácticamente imposible remontar
            if diff >= 2 and min_ >= 75:
                return 0.95   # 2 goles, últimos 15 min
            if diff >= 2 and min_ >= 65:
                return 0.91   # 2 goles, minuto 65+ — sigue siendo alto riesgo
            if diff >= 1 and min_ >= 85:
                return 0.92   # 1 gol, últimos 5 min
            # Todo lo demás: demasiado incierto, no señalizar
            return 0.0

        if home_sim >= away_sim:
            prob = _win_prob(goal_diff, minute)
        else:
            prob = _win_prob(away_goals - home_goals, minute)

        return prob

    # ── Question parsing ───────────────────────────────────────────────────────

    def _is_match_result_question(self, question: str) -> bool:
        """
        Solo procesa preguntas de partido especifico (head-to-head).
        Excluye preguntas de ganador de torneo/liga/copa como
        'Will X win the Carabao Cup?' o 'Will X win the World Cup?'
        que no se pueden predecir con el score de un partido en vivo.
        """
        q = question.lower()

        # Patrones que indican partido especifico con dos equipos
        head_to_head = [
            r"\bbeat\b", r"\bdefeat\b", r"\bvs\.?\b", r"\bversus\b",
        ]
        if any(re.search(p, q) for p in head_to_head):
            return True

        # Excluir preguntas de ganador de torneo/competicion
        tournament_exclusions = [
            r"\bwin\s+the\b",           # "win the Cup / the League"
            r"\bwin\s+\w+\s+cup\b",     # "win Premier Cup"
            r"\bwin\s+\w+\s+league\b",  # "win the La Liga"
            r"\bworld\s+cup\b",         # cualquier World Cup
            r"\bchampionship\b",
            r"\btitle\b",
            r"\bseason\b",
            r"\btournament\b",
            r"\bstandings\b",
        ]
        if any(re.search(p, q) for p in tournament_exclusions):
            return False

        # "win" sin oponente claro → descartar
        return False

    def _parse_team_names(self, question: str) -> list[str]:
        teams = []
        m = re.match(
            r"Will\s+(.+?)\s+(?:beat|defeat|win(?:\s+(?:against|vs\.?|their))?)\s+(.+?)(?:\?|$|\s+on\s|\s+in\s)",
            question, re.I
        )
        if m:
            teams.append(m.group(1).strip())
            if m.group(2):
                team2 = re.sub(r'\s+on\s+.*$', '', m.group(2), flags=re.I).strip()
                team2 = re.sub(r'\s+in\s+.*$', '', team2, flags=re.I).strip()
                if team2:
                    teams.append(team2)
            return teams

        m = re.match(r"Will\s+(.+?)\s+win\b", question, re.I)
        if m:
            teams.append(m.group(1).strip())

        return teams

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _find_live_match(self, matches: list[LiveMatch], teams: list[str]) -> LiveMatch | None:
        best_score = 0.0
        best_match = None

        for match in matches:
            for team_query in teams:
                home_sim = self._name_similarity(team_query, match.home_team)
                away_sim = self._name_similarity(team_query, match.away_team)
                score = max(home_sim, away_sim)
                if score > best_score:
                    best_score = score
                    best_match = match

        return best_match if best_score >= 0.65 else None

    @staticmethod
    def _name_similarity(a: str, b: str) -> float:
        a_words = set(a.lower().split())
        b_words = set(b.lower().split())
        stopwords = {"fc", "cf", "afc", "utd", "united", "city", "the", "de", "sc", "sv", "fk", "sk"}
        a_words -= stopwords
        b_words -= stopwords
        if not a_words or not b_words:
            return 0.0
        intersection = a_words & b_words
        union = a_words | b_words
        jaccard = len(intersection) / len(union)
        contains_bonus = 0.3 if (a.lower() in b.lower() or b.lower() in a.lower()) else 0.0
        return min(1.0, jaccard + contains_bonus)

    def _get_category(self, market: dict) -> str:
        events = market.get("events")
        if events and isinstance(events, list) and events:
            return events[0].get("category", "") or events[0].get("title", "")
        return ""
