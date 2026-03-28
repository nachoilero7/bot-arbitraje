"""
Signal: Betfair Exchange Outright Divergence
Compara precios de Polymarket en outrights contra Betfair Exchange.

Betfair es mejor que Pinnacle para esto porque:
  - Es un exchange — precios fijados por el mercado, sin margen del bookmaker
  - Volumen altisimo en NBA/CL/ligas europeas
  - API gratuita con cuenta registrada (Delayed Data App Key)

Casos de uso:
  - NBA Championship winner
  - Champions League winner
  - Liga winner (EPL, La Liga, Bundesliga, etc.)
"""
import re
import json
from dataclasses import dataclass

from src.signals.base import BaseSignal, Opportunity, SignalType
from src.enrichers.betfair import BetfairClient
from src.utils.logger import get_logger

logger = get_logger(__name__)

OUTRIGHT_PATTERNS = [
    (r"nba|basketball",             "basketball_nba",            ["champion", "championship", "title", "nba finals", "win the nba"]),
    (r"champions league|ucl|cl\b",  "soccer_uefa_champs_league", ["win", "winner", "champion"]),
    (r"premier league|epl",         "soccer_epl",                ["win", "winner", "champion", "title"]),
    (r"la liga|laliga",             "soccer_spain_la_liga",      ["win", "winner", "champion", "title"]),
    (r"bundesliga",                 "soccer_germany_bundesliga", ["win", "winner", "champion", "title"]),
    (r"serie a",                    "soccer_italy_serie_a",      ["win", "winner", "champion", "title"]),
    (r"ligue 1|ligue1",             "soccer_france_ligue_one",   ["win", "winner", "champion", "title"]),
    (r"nfl|super bowl",             "americanfootball_nfl",      ["win", "winner", "super bowl"]),
    (r"mlb|world series",           "baseball_mlb",              ["win", "winner", "world series"]),
    (r"nhl|stanley cup",            "icehockey_nhl",             ["win", "winner", "stanley cup"]),
]


@dataclass
class BetfairOpportunity(Opportunity):
    betfair_prob: float = 0.0
    betfair_odds: float = 0.0
    market_name: str    = ""


class BetfairDivergenceSignal(BaseSignal):

    def __init__(self, betfair_client: BetfairClient, fee_rate: float = 0.02, min_edge: float = 0.03):
        super().__init__(fee_rate=fee_rate, min_edge=min_edge)
        self.betfair = betfair_client

    @property
    def name(self) -> str:
        return "BETFAIR_DIV"

    def detect(self, markets: list[dict], prices: dict = None) -> list[Opportunity]:
        opportunities = []

        for market in markets:
            try:
                question = market.get("question", "")
                category = ""
                events = market.get("events")
                if events and isinstance(events, list) and events:
                    category = events[0].get("category", "") or ""

                sport_key, keywords = self._detect_outright(category, question)
                if not sport_key:
                    continue

                team = self._extract_team(question)
                if not team:
                    continue

                outcome_prices_raw = market.get("outcomePrices")
                if not outcome_prices_raw:
                    continue
                outcome_prices = json.loads(outcome_prices_raw) if isinstance(outcome_prices_raw, str) else outcome_prices_raw
                if len(outcome_prices) < 2:
                    continue
                yes_price = float(outcome_prices[0])
                if yes_price <= 0 or yes_price >= 0.98:
                    continue

                bf_markets = self.betfair.get_outrights(sport_key)
                if not bf_markets:
                    continue

                bf_result = None
                for kw in keywords:
                    bf_result = self.betfair.get_outright_prob(bf_markets, team, kw)
                    if bf_result:
                        break
                if not bf_result:
                    bf_result = self.betfair.get_outright_prob(bf_markets, team)
                if not bf_result:
                    continue

                bf_prob = bf_result.no_vig_prob
                if bf_prob <= 0:
                    continue

                confirmed_edge = bf_prob - yes_price - self.fee_rate
                if confirmed_edge < self.min_edge:
                    continue

                token_ids = json.loads(market.get("clobTokenIds", "[]")) if isinstance(market.get("clobTokenIds"), str) else []

                opp = BetfairOpportunity(
                    signal_type=SignalType.MISPRICED_CORR,
                    condition_id=market.get("conditionId", ""),
                    question=question,
                    category=category,
                    token_id=token_ids[0] if token_ids else "",
                    side="YES",
                    market_price=yes_price,
                    fair_value=bf_prob,
                    edge=round(confirmed_edge, 5),
                    edge_pct=round(confirmed_edge / yes_price, 4) if yes_price > 0 else 0,
                    best_bid=market.get("bestBid") or 0,
                    best_ask=market.get("bestAsk") or 0,
                    spread=market.get("spread") or 0,
                    liquidity_usd=market.get("liquidityNum") or 0,
                    volume_24h=market.get("volume24hr") or 0,
                    betfair_prob=bf_prob,
                    betfair_odds=bf_result.back_price,
                    market_name=bf_result.market_name,
                    notes=(
                        f"Poly={yes_price:.3f} | Betfair={bf_prob:.3f} "
                        f"(@{bf_result.back_price:.2f}) | "
                        f"edge={confirmed_edge:.4f} | {bf_result.market_name}"
                    )
                )
                opportunities.append(opp)
                logger.info(
                    f"[BETFAIR] {question[:50]} | "
                    f"poly={yes_price:.3f} bf={bf_prob:.3f} edge={confirmed_edge:.4f}"
                )

            except Exception as e:
                logger.debug(f"BetfairDivergence skip: {e}")

        opportunities.sort(key=lambda o: o.edge, reverse=True)
        return opportunities

    def _detect_outright(self, category: str, question: str) -> tuple[str, list[str]]:
        text = f"{category} {question}".lower()
        for sport_pattern, sport_key, keywords in OUTRIGHT_PATTERNS:
            if re.search(sport_pattern, text, re.I):
                for kw in keywords:
                    if kw in text:
                        return sport_key, keywords
        return "", []

    def _extract_team(self, question: str) -> str:
        q = question.strip()
        m = re.match(
            r"Will\s+(?:the\s+)?(.+?)\s+(?:win|be|finish|qualify|claim|advance|reach|make)",
            q, re.I
        )
        if m:
            team = m.group(1).strip()
            team = re.sub(r"^(the|a|an)\s+", "", team, flags=re.I).strip()
            if not re.search(r"\b(game|match|series|season|round|game \d)\b", team, re.I):
                return team
        return ""
