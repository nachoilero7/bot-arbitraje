"""
Signal: MiroFish — LLM multi-agente para deteccion de edge en mercados de eventos.

Lee oportunidades desde un cache JSON generado por MiroFishRunner (background).
No llama al LLM directamente — eso es responsabilidad del runner.

Cache format (data/mirofish_cache.json):
{
  "condition_id": {
    "question": "...",
    "category": "...",
    "market_price": 0.45,
    "llm_probability": 0.62,
    "confidence": "medium",
    "edge": 0.15,
    "key_factor": "...",
    "token_id": "...",
    "liquidity_usd": 1200.0,
    "analyzed_at": "2026-03-26T10:00:00"
  }, ...
}
"""
import json
import os
from datetime import datetime, timedelta

from src.signals.base import BaseSignal, Opportunity, SignalType
from src.utils.logger import get_logger

logger = get_logger(__name__)

CACHE_PATH    = "data/mirofish_cache.json"
CACHE_MAX_AGE = timedelta(hours=4)   # descartar analisis con mas de 4h de antigüedad

# Multiplicador de edge segun confianza del LLM
CONFIDENCE_MULT = {"low": 0.5, "medium": 0.8, "high": 1.0}


class MiroFishSignal(BaseSignal):
    """
    Lee el cache de MiroFishRunner y emite Opportunity cuando el LLM detecta
    una discrepancia significativa entre su estimacion y el precio de mercado.
    """

    def __init__(self, fee_rate: float = 0.02, min_edge: float = 0.10):
        # Threshold mas alto que otros signals (LLM tiene mayor incertidumbre)
        super().__init__(fee_rate=fee_rate, min_edge=min_edge)

    @property
    def name(self) -> str:
        return "MIROFISH"

    def detect(self, markets: list[dict], prices: dict = None) -> list[Opportunity]:
        if not os.path.exists(CACHE_PATH):
            return []

        try:
            with open(CACHE_PATH, "r", encoding="utf-8") as f:
                cache: dict = json.load(f)
        except Exception as e:
            logger.warning(f"[MIROFISH] Error leyendo cache: {e}")
            return []

        opportunities = []
        now = datetime.utcnow()
        cutoff = now - CACHE_MAX_AGE

        for condition_id, entry in cache.items():
            try:
                analyzed_at = datetime.fromisoformat(entry.get("analyzed_at", "1970-01-01"))
                if analyzed_at < cutoff:
                    continue  # analisis vencido

                market_price  = float(entry["market_price"])
                llm_prob      = float(entry["llm_probability"])
                confidence    = entry.get("confidence", "low")
                token_id      = entry.get("token_id", "")
                liquidity_usd = float(entry.get("liquidity_usd", 0))

                if not token_id:
                    continue

                # Edge ajustado por confianza del LLM
                raw_edge = llm_prob - market_price - self.fee_rate
                adj_edge = raw_edge * CONFIDENCE_MULT.get(confidence, 0.5)

                if adj_edge < self.min_edge:
                    continue

                # Determinar lado: si LLM cree que es mas probable → comprar YES
                side = "YES" if llm_prob > market_price else "NO"
                if side == "NO":
                    # Para NO: el edge es el inverso
                    raw_edge = (1 - llm_prob) - (1 - market_price) - self.fee_rate
                    adj_edge = raw_edge * CONFIDENCE_MULT.get(confidence, 0.5)
                    if adj_edge < self.min_edge:
                        continue

                opp = Opportunity(
                    signal_type=SignalType.MIROFISH,
                    condition_id=condition_id,
                    question=entry.get("question", ""),
                    category=entry.get("category", ""),
                    token_id=token_id,
                    side=side,
                    market_price=market_price,
                    fair_value=llm_prob,
                    edge=round(adj_edge, 5),
                    edge_pct=round(adj_edge / market_price, 4) if market_price > 0 else 0,
                    liquidity_usd=liquidity_usd,
                    notes=(
                        f"LLM={llm_prob:.3f} mkt={market_price:.3f} "
                        f"conf={confidence} adj_edge={adj_edge:.4f} | "
                        f"{entry.get('key_factor','')[:80]}"
                    ),
                )
                opportunities.append(opp)
                logger.debug(
                    f"[MIROFISH] {opp.question[:55]} | "
                    f"llm={llm_prob:.2f} mkt={market_price:.2f} edge={adj_edge:.3f} [{confidence}]"
                )

            except Exception as e:
                logger.debug(f"[MIROFISH] Skip {condition_id[:12]}: {e}")

        opportunities.sort(key=lambda o: o.edge, reverse=True)
        return opportunities
