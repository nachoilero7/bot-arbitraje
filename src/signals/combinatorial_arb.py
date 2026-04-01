"""
Signal: Combinatorial Arbitrage
Detecta violaciones de constraints logicos entre mercados relacionados.

Principio: si evento A implica evento B, entonces P(A) <= P(B).
Cuando P(A) > P(B), comprar B (mas barato pero con mayor probabilidad real)
y/o vender A (mas caro pero con menor probabilidad real).

Ejemplos de constraints:
  - "BTC above $90k" implica "BTC above $80k"  → P($90k) <= P($80k)
  - "Partido termina con >3 goles" implica "> 1 gol" → P(3+) <= P(1+)
  - "Trump gana por >20 puntos" implica "Trump gana" → P(>20) <= P(gana)

Ademas detecta arbitraje de paridad intra-mercado:
  - YES + NO deberian sumar exactamente $1
  - Si YES=0.45 y NO=0.60 → YES+NO=1.05 → vender ambos (riesgo-free)
  - Si YES=0.45 y NO=0.52 → YES+NO=0.97 → comprar ambos (riesgo-free)

Solo opera intra-paridad cuando la desviacion supera el fee (>2%).
"""
import json
import re
from collections import defaultdict

from src.signals.base import BaseSignal, Opportunity, SignalType
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Minima desviacion de paridad para actuar (YES + NO != 1.0)
MIN_PARITY_DEVIATION = 0.025   # 2.5% — mayor que los fees (2%)

# Minimo edge para detectar constraint violation
MIN_CONSTRAINT_EDGE = 0.04

# Solo analizar mercados con volumen razonable
MIN_VOLUME_CONSTRAINT = 500

# Patrones de agrupacion por tema (para buscar mercados relacionados)
THRESHOLD_PATTERNS = [
    # Precios de crypto
    (r"(?:above|over|exceed|surpass|reach|hit)\s+\$?([\d,]+)k?", "price_threshold"),
    # Numeros de votos/puntos/porcentaje
    (r"(?:more than|over|above|at least)\s+([\d]+)\s*(?:percent|%|points?|seats?|votes?)", "count_threshold"),
    # Posiciones (top N, place, rank)
    (r"(?:top|finish in)\s+([\d]+)", "rank_threshold"),
    # Goles en deportes
    (r"(?:over|more than|at least)\s+([\d]+)\s*(?:goals?|points?|runs?)", "goals_threshold"),
]


class CombinatorialArbSignal(BaseSignal):
    """
    Detecta dos tipos de arbitraje combinatorio:
    1. Intra-paridad: YES + NO != $1.00 en el mismo mercado
    2. Cross-market constraints: mercados relacionados con precios incoherentes
    """

    def __init__(self, fee_rate: float = 0.02, min_edge: float = 0.03):
        super().__init__(fee_rate=fee_rate, min_edge=min_edge)

    @property
    def name(self) -> str:
        return "COMBINATORIAL_ARB"

    def detect(self, markets: list[dict], prices: dict = None) -> list[Opportunity]:
        opportunities = []

        # Tipo 1: paridad intra-mercado
        opportunities.extend(self._detect_parity_violations(markets))

        # Tipo 2: constraint violations entre mercados relacionados (thresholds numericos)
        opportunities.extend(self._detect_constraint_violations(markets))

        # Tipo 3: mercados mutuamente excluyentes que suman > 1.0
        opportunities.extend(self._detect_mutual_exclusion_violations(markets))

        # Tipo 4: mercados mutuamente excluyentes que suman < 1.0 (arb puro multi-outcome)
        opportunities.extend(self._detect_multioutcome_parity(markets))

        opportunities.sort(key=lambda o: o.edge, reverse=True)
        return opportunities

    # ── Tipo 1: Paridad intra-mercado ───────────────────────────────────────────

    def _detect_parity_violations(self, markets: list[dict]) -> list[Opportunity]:
        """
        YES + NO deberia ser exactamente 1.0.
        - Si suma < 1.0: comprar ambos (risk-free profit = 1 - suma)
        - Si suma > 1.0: raro, pero vendible si tenemos posicion existente
        """
        opportunities = []

        for market in markets:
            try:
                volume = market.get("volume24hr") or 0
                if volume < MIN_VOLUME_CONSTRAINT:
                    continue

                outcome_prices_raw = market.get("outcomePrices")
                if not outcome_prices_raw:
                    continue
                prices_list = json.loads(outcome_prices_raw) if isinstance(outcome_prices_raw, str) else outcome_prices_raw
                if len(prices_list) != 2:
                    continue

                yes_price = float(prices_list[0])
                no_price  = float(prices_list[1])
                if yes_price <= 0 or no_price <= 0:
                    continue

                parity_sum = yes_price + no_price
                deviation  = abs(1.0 - parity_sum)

                if deviation < MIN_PARITY_DEVIATION:
                    continue

                token_ids = json.loads(market.get("clobTokenIds", "[]")) if isinstance(market.get("clobTokenIds"), str) else []
                question  = market.get("question", "")
                category  = self._get_category(market)
                liquidity = market.get("liquidityNum") or 0

                if parity_sum < 1.0:
                    # Comprar YES + NO → recibir $1 → profit = 1 - suma - 2*fee
                    edge = (1.0 - parity_sum) - 2 * self.fee_rate
                    if edge < self.min_edge:
                        continue

                    # Senialar el lado mas barato primero (YES generalmente)
                    cheaper_side  = "YES" if yes_price <= no_price else "NO"
                    cheaper_price = yes_price if cheaper_side == "YES" else no_price
                    cheaper_token = token_ids[0] if cheaper_side == "YES" and token_ids else (token_ids[1] if token_ids else "")

                    opp = Opportunity(
                        signal_type=SignalType.PARITY,
                        condition_id=market.get("conditionId", ""),
                        question=question,
                        category=category,
                        token_id=cheaper_token,
                        side=cheaper_side,
                        market_price=cheaper_price,
                        fair_value=round(1.0 - (no_price if cheaper_side == "YES" else yes_price), 5),
                        edge=round(edge, 5),
                        edge_pct=round(edge / cheaper_price, 4) if cheaper_price > 0 else 0,
                        best_bid=market.get("bestBid") or 0,
                        best_ask=market.get("bestAsk") or 0,
                        spread=market.get("spread") or 0,
                        liquidity_usd=liquidity,
                        volume_24h=volume,
                        notes=(
                            f"PARITY VIOLATION | YES={yes_price:.3f} + NO={no_price:.3f} = {parity_sum:.3f} "
                            f"(sum < 1) | edge={edge:.4f} | comprar ambos lados"
                        ),
                    )
                    opportunities.append(opp)
                    logger.info(
                        f"[COMB_ARB] Parity violation {question[:50]} | "
                        f"YES+NO={parity_sum:.3f} edge={edge:.4f}"
                    )

            except Exception as e:
                logger.debug(f"CombiArb parity skip {market.get('conditionId','?')[:12]}: {e}")

        return opportunities

    # ── Tipo 2: Cross-market constraint violations ──────────────────────────────

    def _detect_constraint_violations(self, markets: list[dict]) -> list[Opportunity]:
        """
        Agrupa mercados por tema y busca violaciones de constraints logicos.
        Ej: 'BTC above $90k' no puede ser mas probable que 'BTC above $80k'.
        """
        opportunities = []

        # Filtrar mercados con suficiente volumen
        eligible = [m for m in markets if (m.get("volume24hr") or 0) >= MIN_VOLUME_CONSTRAINT]

        # Agrupar por patron de threshold dentro del mismo tema
        groups = self._group_by_topic(eligible)

        for topic, group in groups.items():
            if len(group) < 2:
                continue

            # Ordenar por threshold numerico
            sorted_group = sorted(group, key=lambda x: x["threshold"])

            # Verificar constraints: P(mayor) <= P(menor)
            for i in range(len(sorted_group) - 1):
                lower  = sorted_group[i]    # threshold menor → deberia tener P mayor
                higher = sorted_group[i + 1] # threshold mayor → deberia tener P menor

                p_lower  = lower["yes_price"]
                p_higher = higher["yes_price"]

                if p_lower <= 0 or p_higher <= 0:
                    continue

                # Constraint: P(higher threshold) <= P(lower threshold)
                if p_higher > p_lower:
                    # Violation! El mercado con threshold mayor es mas caro
                    edge = (p_higher - p_lower) - MIN_CONSTRAINT_EDGE

                    if edge < self.min_edge:
                        continue

                    # Oportunidad: comprar el lower (mas barato, mas probable)
                    market = lower["market"]
                    token_ids = json.loads(market.get("clobTokenIds", "[]")) if isinstance(market.get("clobTokenIds"), str) else []

                    opp = Opportunity(
                        signal_type=SignalType.MISPRICED_CORR,
                        condition_id=market.get("conditionId", ""),
                        question=market.get("question", ""),
                        category=self._get_category(market),
                        token_id=token_ids[0] if token_ids else "",
                        side="YES",
                        market_price=p_lower,
                        fair_value=round(p_higher, 5),  # deberia valer al menos lo que vale el higher
                        edge=round(edge, 5),
                        edge_pct=round(edge / p_lower, 4) if p_lower > 0 else 0,
                        best_bid=market.get("bestBid") or 0,
                        best_ask=market.get("bestAsk") or 0,
                        spread=market.get("spread") or 0,
                        liquidity_usd=market.get("liquidityNum") or 0,
                        volume_24h=market.get("volume24hr") or 0,
                        notes=(
                            f"CONSTRAINT VIOLATION | topic={topic} | "
                            f"threshold_{lower['threshold']} P={p_lower:.3f} < "
                            f"threshold_{higher['threshold']} P={p_higher:.3f} | "
                            f"edge={edge:.4f}"
                        ),
                    )
                    opportunities.append(opp)
                    logger.info(
                        f"[COMB_ARB] Constraint violation | {topic} | "
                        f"P({lower['threshold']})={p_lower:.3f} < P({higher['threshold']})={p_higher:.3f} | "
                        f"edge={edge:.4f}"
                    )

        return opportunities

    def _group_by_topic(self, markets: list[dict]) -> dict:
        """
        Agrupa mercados que parecen ser del mismo evento pero con distintos umbrales.
        Clave: (evento_base, tipo_threshold)
        """
        groups: dict[str, list] = defaultdict(list)

        for market in markets:
            question = market.get("question", "")
            if not question:
                continue

            outcome_prices_raw = market.get("outcomePrices")
            if not outcome_prices_raw:
                continue
            try:
                prices_list = json.loads(outcome_prices_raw) if isinstance(outcome_prices_raw, str) else outcome_prices_raw
                yes_price = float(prices_list[0]) if prices_list else 0
                if yes_price <= 0:
                    continue
            except Exception:
                continue

            for pattern, ptype in THRESHOLD_PATTERNS:
                m = re.search(pattern, question, re.I)
                if not m:
                    continue

                threshold_str = m.group(1).replace(",", "")
                try:
                    threshold = float(threshold_str)
                except ValueError:
                    continue

                # Generar clave de agrupacion: pregunta sin el numero del threshold
                base = re.sub(pattern, "THRESHOLD", question, flags=re.I)
                base = re.sub(r'\s+', ' ', base).strip().lower()[:80]
                group_key = f"{ptype}|{base}"

                groups[group_key].append({
                    "market":    market,
                    "threshold": threshold,
                    "yes_price": yes_price,
                    "question":  question,
                })
                break  # un patron por mercado es suficiente

        return groups

    # ── Helpers de agrupacion ───────────────────────────────────────────────────

    # Palabras clave que indican que NO hay exclusion mutua
    # (multiples outcomes pueden ocurrir simultaneamente)
    _NON_EXCLUSIVE_KEYWORDS = (
        "qualify", "advance", "reach the", "make the", "make it to",
        "go to", "participate", "be relegated", "get relegated",
    )

    def _group_exclusive_markets(self, markets: list[dict]) -> dict[str, list]:
        """
        Agrupa mercados binarios YES/NO que pertenecen al mismo evento mutuamente
        excluyente (elecciones, premios, torneos con un solo ganador).

        Clave de grupo: estructura de la pregunta con nombres propios reemplazados
        por NAME → agrupa 'Will Orban be PM?' y 'Will Magyar be PM?' juntos.
        """
        groups: dict[str, list] = defaultdict(list)

        for market in markets:
            question = market.get("question", "")
            if not question:
                continue
            if (market.get("volume24hr") or 0) < MIN_VOLUME_CONSTRAINT:
                continue

            outcome_prices_raw = market.get("outcomePrices")
            if not outcome_prices_raw:
                continue
            try:
                prices_list = json.loads(outcome_prices_raw) if isinstance(outcome_prices_raw, str) else outcome_prices_raw
                yes_price = float(prices_list[0]) if prices_list else 0
                if yes_price <= 0 or yes_price >= 1:
                    continue
            except Exception:
                continue

            q_lower = question.lower()
            if any(kw in q_lower for kw in self._NON_EXCLUSIVE_KEYWORDS):
                continue

            base = re.sub(r'\b[A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*\b', 'NAME', question)
            base = re.sub(r'\s+', ' ', base).strip().lower()[:100]
            if not base or len(base) < 15:
                continue

            groups[base].append({
                "market":    market,
                "yes_price": yes_price,
                "question":  question,
            })

        return groups

    # ── Tipo 3: Mercados mutuamente excluyentes (suma > 1.0) ───────────────────

    def _detect_mutual_exclusion_violations(self, markets: list[dict]) -> list[Opportunity]:
        """
        Detecta grupos mutuamente excluyentes donde suma YES > 1.0 + fees:
        al menos uno esta sobrevaluado → apostar NO en el mas caro.
        """
        opportunities = []
        groups = self._group_exclusive_markets(markets)

        for base_q, group in groups.items():
            if len(group) < 2:
                continue

            total_prob = sum(m["yes_price"] for m in group)
            edge = total_prob - 1.0 - 2 * self.fee_rate
            if edge < self.min_edge:
                continue

            most_overpriced = max(group, key=lambda x: x["yes_price"])
            market = most_overpriced["market"]
            token_ids = json.loads(market.get("clobTokenIds", "[]")) if isinstance(market.get("clobTokenIds"), str) else []
            no_token = token_ids[1] if len(token_ids) > 1 else ""
            no_price = 1.0 - most_overpriced["yes_price"]
            fair_no  = no_price - edge

            opp = Opportunity(
                signal_type=SignalType.MISPRICED_CORR,
                condition_id=market.get("conditionId", ""),
                question=market.get("question", ""),
                category=self._get_category(market),
                token_id=no_token,
                side="NO",
                market_price=no_price,
                fair_value=round(fair_no, 5),
                edge=round(edge, 5),
                edge_pct=round(edge / no_price, 4) if no_price > 0 else 0,
                best_bid=market.get("bestBid") or 0,
                best_ask=market.get("bestAsk") or 0,
                spread=market.get("spread") or 0,
                liquidity_usd=market.get("liquidityNum") or 0,
                volume_24h=market.get("volume24hr") or 0,
                notes=(
                    f"MUTUAL EXCLUSION | grupo={len(group)} mercados | "
                    f"suma_probs={total_prob:.3f} > 1.0 | edge={edge:.4f} | "
                    f"mercados: {[m['question'][:30] for m in group]}"
                ),
            )
            opportunities.append(opp)
            logger.info(
                f"[COMB_ARB] Mutual exclusion violation | {len(group)} markets | "
                f"sum={total_prob:.3f} edge={edge:.4f} | base='{base_q[:50]}'"
            )

        return opportunities

    # ── Tipo 4: Multi-outcome parity (suma < 1.0) ──────────────────────────────

    def _detect_multioutcome_parity(self, markets: list[dict]) -> list[Opportunity]:
        """
        Detecta grupos mutuamente excluyentes donde la suma de precios YES es
        MENOR que 1.0 - N*fees → comprar YES en TODOS garantiza profit sin
        importar quien gana (arbitraje puro multi-outcome).

        Requiere que el grupo capture la mayoria de la probabilidad total
        (suma >= 0.60) para evitar falsos positivos con longshots parciales
        (ej: 2 de 100 golfistas no forman un grupo valido).

        Ejemplo valido:
          Eleccion con 3 candidatos: A=0.28, B=0.35, C=0.22 → suma=0.85
          Fees totales: 3 * 0.02 = 0.06
          Bundle edge: 1.0 - 0.85 - 0.06 = 0.09 → arb positivo
          → comprar YES en los 3 → siempre cobras $1 → profit garantizado

        Crea una Opportunity por mercado del grupo (misma oportunidad, distintos tokens).
        El executor debe intentar tomar TODAS para que el arb sea risk-free.
        """
        # Suma minima del grupo para garantizar que capturamos la mayoria del campo
        MIN_GROUP_PROB = 0.60

        opportunities = []
        groups = self._group_exclusive_markets(markets)

        for base_q, group in groups.items():
            if len(group) < 2:
                continue

            total_prob = sum(m["yes_price"] for m in group)
            n = len(group)

            # Descartar grupos que no capturan suficiente probabilidad
            # (evita agrupar 2 longshots de 100 jugadores)
            if total_prob < MIN_GROUP_PROB:
                continue

            total_fees = n * self.fee_rate
            bundle_edge = 1.0 - total_prob - total_fees
            if bundle_edge < self.min_edge:
                continue

            # Crear una Opportunity por cada mercado del grupo
            group_questions = [m["question"][:35] for m in group]
            for item in group:
                market    = item["market"]
                yes_price = item["yes_price"]
                token_ids = json.loads(market.get("clobTokenIds", "[]")) if isinstance(market.get("clobTokenIds"), str) else []
                yes_token = token_ids[0] if token_ids else ""

                # Valor justo proporcional: si la suma total fuera 1.0,
                # cada candidato valdria yes_price / total_prob
                fair_value = round(yes_price / total_prob, 5)

                opp = Opportunity(
                    signal_type=SignalType.PARITY,
                    condition_id=market.get("conditionId", ""),
                    question=market.get("question", ""),
                    category=self._get_category(market),
                    token_id=yes_token,
                    side="YES",
                    market_price=yes_price,
                    fair_value=fair_value,
                    edge=round(bundle_edge, 5),
                    edge_pct=round(bundle_edge / total_prob, 4) if total_prob > 0 else 0,
                    best_bid=market.get("bestBid") or 0,
                    best_ask=market.get("bestAsk") or 0,
                    spread=market.get("spread") or 0,
                    liquidity_usd=market.get("liquidityNum") or 0,
                    volume_24h=market.get("volume24hr") or 0,
                    notes=(
                        f"MULTI-OUTCOME PARITY | {n} candidatos | "
                        f"suma={total_prob:.3f} | bundle_edge={bundle_edge:.4f} | "
                        f"REQUIERE comprar YES en todos: {group_questions}"
                    ),
                )
                opportunities.append(opp)

            logger.info(
                f"[COMB_ARB] Multi-outcome parity | {n} markets | "
                f"sum={total_prob:.3f} bundle_edge={bundle_edge:.4f} | base='{base_q[:50]}'"
            )

        return opportunities

    def _get_category(self, market: dict) -> str:
        events = market.get("events")
        if events and isinstance(events, list) and events:
            return events[0].get("category", "") or events[0].get("title", "")
        return ""
