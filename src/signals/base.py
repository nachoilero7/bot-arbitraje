"""
Interfaz base para todos los detectores de edge.
Cada signal implementa detect() y retorna una lista de Opportunity.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class SignalType(str, Enum):
    PARITY           = "PARITY"           # YES + NO < $1 (arbitrage puro)
    SPREAD_CAPTURE   = "SPREAD_CAPTURE"   # Bid-ask spread amplio, market making
    OVERPRICED_NO    = "OVERPRICED_NO"    # NO sobrevaluado vs probabilidad base
    RESOLUTION_LAG   = "RESOLUTION_LAG"   # Mercado lento actualizando outcome claro
    MISPRICED_CORR   = "MISPRICED_CORR"   # Mercados correlacionados con precio incoherente
    PRICE_DRIFT      = "PRICE_DRIFT"      # Momentum/mean-reversion por movimiento consistente
    CALIBRATION_BIAS = "CALIBRATION_BIAS" # Sesgo de calibracion documentado (SSRN 5910522, 124M trades)


@dataclass
class Opportunity:
    """Una oportunidad de edge detectada en un mercado."""
    signal_type: SignalType
    condition_id: str
    question: str
    category: str

    # Token a operar
    token_id: str
    side: str              # "YES", "NO", o "YES+NO" (parity arb)
    market_price: float    # Precio actual del token principal (YES para parity, mid para otros)
    fair_value: float      # Nuestra estimacion del valor justo

    # Metricas de edge
    edge: float            # fair_value - market_price (si > 0, hay ventaja)
    edge_pct: float        # edge como % del precio

    # Segundo token (solo para parity arb YES+NO) — con defaults para no romper otros signals
    token_id_b: str = ""   # NO token
    price_b: float = 0.0   # Precio del token secundario (NO price para parity)

    # Liquidez
    best_bid: float = 0.0
    best_ask: float = 0.0
    spread: float = 0.0
    liquidity_usd: float = 0.0
    volume_24h: float = 0.0

    # Metadata
    detected_at: datetime = field(default_factory=datetime.utcnow)
    notes: str = ""

    def is_actionable(self, min_edge: float = 0.03) -> bool:
        return self.edge >= min_edge and self.market_price > 0


class BaseSignal(ABC):
    """
    Clase base para todos los detectores de oportunidades.
    Cada implementacion analiza mercados desde una perspectiva distinta.
    """

    def __init__(self, fee_rate: float = 0.02, min_edge: float = 0.03):
        self.fee_rate = fee_rate
        self.min_edge = min_edge

    @property
    @abstractmethod
    def name(self) -> str:
        """Nombre identificador del signal."""
        ...

    @abstractmethod
    def detect(self, markets: list[dict], prices: dict) -> list[Opportunity]:
        """
        Analiza los mercados y retorna oportunidades encontradas.

        Args:
            markets: Lista de mercados de get_all_active_markets()
            prices:  Dict con precios por token_id { token_id: { bid, ask, mid, spread } }

        Returns:
            Lista de Opportunity ordenada por edge desc
        """
        ...
