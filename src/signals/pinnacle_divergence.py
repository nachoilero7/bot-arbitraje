"""
Signal: Pinnacle Outright Divergence
Compara precios de Polymarket en mercados de outrights/futures contra
las probabilidades de Pinnacle Sports (el bookmaker mas sharp del mundo).

Casos de uso principales:
  - NBA Championship winner: Thunder, Celtics, Knicks, etc.
  - UEFA Champions League winner: Real Madrid, Arsenal, etc.
  - Liga winner: Man City, Barcelona, Bayern, etc.
  - NFL Super Bowl winner, MLB World Series, etc.

Por que Pinnacle es ideal:
  - No cierra cuentas ganadoras (calibra el mercado, no extrae a recreacionales)
  - Vig ~2% (vs 5-10% soft books) => probabilidades mas cercanas a la realidad
  - Precio Pinnacle = consenso del mercado global de sports betting

Logica:
  1. Para cada market de Polymarket que es un outright (CL winner, NBA champ, etc.)
  2. Extraer el equipo/jugador del question
  3. Buscar en Pinnacle specials el precio correspondiente
  4. Calcular no-vig prob de Pinnacle
  5. Si poly_price < pinnacle_prob - fee => edge confirmado
"""
import re
import json
from dataclasses import dataclass

from src.signals.base import BaseSignal, Opportunity, SignalType
from src.enrichers.pinnacle import PinnacleApiClient, SPORT_IDS
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Patrones para detectar el tipo de outright y sport desde la pregunta
OUTRIGHT_PATTERNS = [
    # NBA
    (r"nba|basketball", "basketball_nba", ["champion", "championship", "title", "win the nba", "nba finals"]),
    # Champions League
    (r"champions league|ucl|cl\b", "soccer_uefa_champs_league", ["win", "winner", "champion"]),
    # Premier League
    (r"premier league|epl", "soccer_epl", ["win", "winner", "champion", "title"]),
    # La Liga
    (r"la liga|laliga", "soccer_spain_la_liga", ["win", "winner", "champion", "title"]),
    # Bundesliga
    (r"bundesliga", "soccer_germany_bundesliga", ["win", "winner", "champion", "title"]),
    # Serie A
    (r"serie a", "soccer_italy_serie_a", ["win", "winner", "champion", "title"]),
    # Ligue 1
    (r"ligue 1|ligue1", "soccer_france_ligue_one", ["win", "winner", "champion", "title"]),
    # NFL
    (r"nfl|super bowl", "americanfootball_nfl", ["win", "winner", "super bowl"]),
    # MLB
    (r"mlb|world series", "baseball_mlb", ["win", "winner", "world series"]),
    # NHL
    (r"nhl|stanley cup", "icehockey_nhl", ["win", "winner", "stanley cup"]),
]


@dataclass
class PinnacleOpportunity(Opportunity):
    pinnacle_prob: float = 0.0
    pinnacle_odds: float = 0.0
    market_name: str = ""
    num_contestants: int = 0


class PinnacleDivergenceSignal(BaseSignal):

    def __init__(self, pinnacle_client: PinnacleApiClient, fee_rate: float = 0.02, min_edge: float = 0.03):
        super().__init__(fee_rate=fee_rate, min_edge=min_edge)
        self.pinnacle = pinnacle_client
        self._specials_cache: dict[str, list] = {}  # sport_key -> specials (con TTL propio en cliente)

    @property
    def name(self) -> str:
        return "PINNACLE_DIV"

    def _get_specials(self, sport_key: str) -> list[dict]:
        """Obtiene specials de Pinnacle (con cache interna del cliente)."""
        try:
            return self.pinnacle.get_specials_for_sport_key(sport_key)
        except Exception as e:
            logger.warning(f"Pinnacle specials failed for {sport_key}: {e}")
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

                # Detectar si es un outright y de que sport
                sport_key, market_keywords = self._detect_outright(category, question)
                if not sport_key:
                    continue

                # Extraer el equipo/jugador de la pregunta
                team = self._extract_team(question)
                if not team:
                    continue

                # Obtener precio de Polymarket (YES token)
                outcome_prices_raw = market.get("outcomePrices")
                if not outcome_prices_raw:
                    continue
                outcome_prices = json.loads(outcome_prices_raw) if isinstance(outcome_prices_raw, str) else outcome_prices_raw
                if len(outcome_prices) < 2:
                    continue
                yes_price = float(outcome_prices[0])
                if yes_price <= 0 or yes_price >= 0.98:
                    continue

                # Buscar en Pinnacle
                specials = self._get_specials(sport_key)
                if not specials:
                    continue

                # Intentar con keyword del mercado y sin keyword
                pinnacle_result = None
                for kw in market_keywords:
                    pinnacle_result = self.pinnacle.get_outright_prob(specials, team, kw)
                    if pinnacle_result:
                        break
                if not pinnacle_result:
                    pinnacle_result = self.pinnacle.get_outright_prob(specials, team)
                if not pinnacle_result:
                    continue

                pin_prob = pinnacle_result.no_vig_prob
                if pin_prob <= 0:
                    continue

                confirmed_edge = pin_prob - yes_price - self.fee_rate
                if confirmed_edge < self.min_edge:
                    continue

                token_ids = json.loads(market.get("clobTokenIds", "[]")) if isinstance(market.get("clobTokenIds"), str) else []

                opp = PinnacleOpportunity(
                    signal_type=SignalType.MISPRICED_CORR,
                    condition_id=market.get("conditionId", ""),
                    question=question,
                    category=category,
                    token_id=token_ids[0] if token_ids else "",
                    side="YES",
                    market_price=yes_price,
                    fair_value=pin_prob,
                    edge=round(confirmed_edge, 5),
                    edge_pct=round(confirmed_edge / yes_price, 4) if yes_price > 0 else 0,
                    best_bid=market.get("bestBid") or 0,
                    best_ask=market.get("bestAsk") or 0,
                    spread=market.get("spread") or 0,
                    liquidity_usd=market.get("liquidityNum") or 0,
                    volume_24h=market.get("volume24hr") or 0,
                    pinnacle_prob=pin_prob,
                    pinnacle_odds=pinnacle_result.decimal_odds,
                    market_name=pinnacle_result.market_name,
                    num_contestants=pinnacle_result.num_contestants,
                    notes=(
                        f"Poly={yes_price:.3f} | Pinnacle={pin_prob:.3f} "
                        f"(@{pinnacle_result.decimal_odds:.2f}) | "
                        f"edge={confirmed_edge:.4f} | {pinnacle_result.market_name}"
                    )
                )
                opportunities.append(opp)
                logger.info(
                    f"[PINNACLE] {question[:50]} | "
                    f"poly={yes_price:.3f} pin={pin_prob:.3f} edge={confirmed_edge:.4f}"
                )

            except Exception as e:
                logger.debug(f"PinnacleDivergence skip: {e}")

        opportunities.sort(key=lambda o: o.edge, reverse=True)
        return opportunities

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _detect_outright(self, category: str, question: str) -> tuple[str, list[str]]:
        """Retorna (sport_key, market_keywords) si el mercado parece un outright."""
        text = f"{category} {question}".lower()
        for sport_pattern, sport_key, keywords in OUTRIGHT_PATTERNS:
            if re.search(sport_pattern, text, re.I):
                # Verificar que la pregunta tenga un keyword de outright
                for kw in keywords:
                    if kw in text:
                        return sport_key, keywords
        return "", []

    def _extract_team(self, question: str) -> str:
        """
        Extrae el nombre del equipo/jugador de una pregunta de outright.

        Patrones:
          "Will the Oklahoma City Thunder win the NBA Championship?"
          -> "Oklahoma City Thunder"

          "Will Real Madrid win the Champions League?"
          -> "Real Madrid"

          "Will Arsenal win the 2024/25 Champions League?"
          -> "Arsenal"
        """
        q = question.strip()

        # Patron: "Will TEAM win/be/..."
        m = re.match(
            r"Will\s+(?:the\s+)?(.+?)\s+(?:win|be|finish|qualify|claim|advance|reach|make)",
            q, re.I
        )
        if m:
            team = m.group(1).strip()
            # Limpiar articulos residuales al inicio
            team = re.sub(r"^(the|a|an)\s+", "", team, flags=re.I).strip()
            # Descartar si parece ser una pregunta de otro tipo
            if not re.search(r"\b(game|match|series|season|round|game \d)\b", team, re.I):
                return team

        return ""
