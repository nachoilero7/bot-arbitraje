"""
Kelly Criterion para position sizing.

Formula Kelly completo:
    f* = (bp - q) / b
    donde:
        b = net odds (cuanto ganamos por cada $1 apostado)
        p = probabilidad estimada de ganar
        q = 1 - p

Para Polymarket (mercado a precio decimal):
    Si compramos YES a precio 'p_market' y nuestra estimacion es 'p_true':
        b = (1 - p_market) / p_market   (odds implicitos)
        f* = p_true - p_market * (1 - p_true + p_true)
           = p_true - p_market   (simplificado para mercados binarios)

Usamos Fractional Kelly (kelly_fraction = 0.25 por default) para reducir varianza.
"""
from dataclasses import dataclass


@dataclass
class KellyResult:
    kelly_full: float      # Kelly completo (teorico)
    kelly_frac: float      # Kelly fraccional (lo que realmente usamos)
    position_usd: float    # Monto en USD a apostar
    edge: float
    b_odds: float
    p_true: float
    p_market: float


def calculate_kelly(
    p_true: float,
    p_market: float,
    bankroll_usd: float,
    kelly_fraction: float = 0.25,
    max_position_usd: float = 50.0,
) -> KellyResult:
    """
    Calcula el tamano optimo de posicion segun Kelly Criterion.

    Args:
        p_true:          Probabilidad estimada por nuestro modelo
        p_market:        Precio actual en el mercado (= probabilidad implicita)
        bankroll_usd:    Capital total disponible
        kelly_fraction:  Factor de reduccion de riesgo (0.25 = 25% del Kelly completo)
        max_position_usd: Limite maximo por posicion en USD

    Returns:
        KellyResult con el monto recomendado
    """
    if p_market <= 0 or p_market >= 1:
        return KellyResult(0, 0, 0, 0, 0, p_true, p_market)

    # Odds implicitos del mercado
    b = (1 - p_market) / p_market

    # Kelly formula: f* = (bp - q) / b = p - q/b
    q = 1 - p_true
    kelly_full = (b * p_true - q) / b

    # Si Kelly es negativo, no hay edge
    if kelly_full <= 0:
        return KellyResult(kelly_full, 0, 0, p_true - p_market, b, p_true, p_market)

    kelly_frac = kelly_full * kelly_fraction
    position_usd = min(bankroll_usd * kelly_frac, max_position_usd)

    return KellyResult(
        kelly_full=round(kelly_full, 4),
        kelly_frac=round(kelly_frac, 4),
        position_usd=round(position_usd, 2),
        edge=round(p_true - p_market, 4),
        b_odds=round(b, 4),
        p_true=p_true,
        p_market=p_market,
    )
