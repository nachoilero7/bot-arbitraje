"""
BTC P&L Tracker — Registra posiciones abiertas del BTC monitor y calcula
el resultado real cuando el mercado se resuelve.

Para cada trade (real o dry-run) registra la entrada y luego monitorea
periodicamente la resolucion del mercado para calcular P&L real.

CSV: data/btc_pnl.csv
Columnas: entered_at, resolved_at, condition_id, question, side,
          entry_price, fair_value, edge, size_usd, dry_run,
          outcome, exit_price, profit_usd, roi_pct
"""
import csv
import json
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

from src.utils.logger import get_logger

logger = get_logger(__name__)

GAMMA_MARKET_URL = "https://gamma-api.polymarket.com/markets"
POLL_INTERVAL_SECS = 5 * 60   # revisar posiciones abiertas cada 5 minutos
PNL_CSV_PATH = "data/btc_pnl.csv"

CSV_HEADERS = [
    "entered_at", "resolved_at", "condition_id", "question",
    "side", "entry_price", "fair_value", "edge", "size_usd",
    "dry_run", "outcome", "exit_price", "profit_usd", "roi_pct",
]


@dataclass
class OpenPosition:
    entered_at: str
    condition_id: str
    question: str
    side: str           # "UP" o "DOWN"
    entry_price: float
    fair_value: float
    edge: float
    size_usd: float
    dry_run: bool
    token_index: int    # 0=UP, 1=DOWN en outcomePrices


class BtcPnlTracker:
    """
    Thread separado que vigila posiciones abiertas del BTC monitor.
    Cada 5 minutos consulta la Gamma API; cuando el mercado cierra
    calcula P&L y lo escribe en btc_pnl.csv.
    """

    def __init__(self, csv_path: str = PNL_CSV_PATH):
        self.csv_path = Path(csv_path)
        self._positions: list[OpenPosition] = []
        self._lock = threading.Lock()
        self._running = False
        self._ensure_csv()

    # ── Public ─────────────────────────────────────────────────────────────────

    def start(self):
        self._running = True
        threading.Thread(target=self._poll_loop, daemon=True, name="btc-pnl").start()
        logger.info("[BTC P&L] Tracker iniciado")

    def stop(self):
        self._running = False

    def record_entry(
        self,
        condition_id: str,
        question: str,
        side: str,
        entry_price: float,
        fair_value: float,
        edge: float,
        size_usd: float,
        dry_run: bool,
    ):
        """Registra una nueva posicion abierta para monitorear."""
        pos = OpenPosition(
            entered_at=datetime.now(timezone.utc).isoformat(),
            condition_id=condition_id,
            question=question,
            side=side,
            entry_price=entry_price,
            fair_value=fair_value,
            edge=edge,
            size_usd=size_usd,
            dry_run=dry_run,
            token_index=0 if side == "UP" else 1,
        )
        with self._lock:
            self._positions.append(pos)

        mode = "DRY" if dry_run else "LIVE"
        logger.info(
            f"[BTC P&L {mode}] Posicion registrada: {side} @ {entry_price:.3f} | "
            f"${size_usd:.2f} | fair={fair_value:.3f} | edge={edge:+.4f}"
        )

    def open_count(self) -> int:
        with self._lock:
            return len(self._positions)

    # ── Poll loop ───────────────────────────────────────────────────────────────

    def _poll_loop(self):
        while self._running:
            time.sleep(POLL_INTERVAL_SECS)
            if not self._running:
                break

            with self._lock:
                positions = list(self._positions)

            if not positions:
                continue

            resolved = []
            for pos in positions:
                result = self._check_resolution(pos)
                if result:
                    resolved.append((pos, result))

            with self._lock:
                for pos, result in resolved:
                    if pos in self._positions:
                        self._positions.remove(pos)
                        self._write_result(pos, result)

            if resolved:
                logger.info(f"[BTC P&L] {len(resolved)} posicion(es) resueltas")

    # ── Resolution check ────────────────────────────────────────────────────────

    def _check_resolution(self, pos: OpenPosition) -> Optional[dict]:
        """
        Consulta la Gamma API por conditionId.
        Retorna dict con resultado si el mercado cerro, None si aun abierto.
        """
        try:
            r = requests.get(
                GAMMA_MARKET_URL,
                params={"conditionIds": pos.condition_id},
                timeout=10,
            )
            if r.status_code != 200:
                return None

            data = r.json()
            if not data:
                return None
            market = data[0] if isinstance(data, list) else data

            # Solo procesar si se resolvio definitivamente en on-chain.
            # "closed" = ventana terminada pero puede no tener precios finales aun.
            # "resolved" = resultado confirmado en contrato (outcomePrices suma a 1.0).
            if not market.get("resolved", False):
                return None

            # Precio final del token que compramos
            outcome_prices_raw = market.get("outcomePrices", "[0.5,0.5]")
            prices = json.loads(outcome_prices_raw) if isinstance(outcome_prices_raw, str) else outcome_prices_raw
            exit_price = float(prices[pos.token_index]) if len(prices) > pos.token_index else 0.0

            # Sanidad: si ambos tokens valen 0 el mercado no esta resuelto todavia
            total = sum(float(p) for p in prices) if prices else 0.0
            if total < 0.5:
                return None

            # Determinar resultado
            if exit_price >= 0.95:
                outcome = "WIN"
            elif exit_price <= 0.05:
                outcome = "LOSS"
            else:
                outcome = "PARTIAL"

            # P&L: compramos N tokens a entry_price, cada token vale exit_price al cierre
            tokens = pos.size_usd / pos.entry_price
            proceeds = tokens * exit_price
            profit_usd = proceeds - pos.size_usd
            roi_pct = (profit_usd / pos.size_usd) * 100 if pos.size_usd > 0 else 0.0

            return {
                "resolved_at": datetime.now(timezone.utc).isoformat(),
                "outcome": outcome,
                "exit_price": round(exit_price, 4),
                "profit_usd": round(profit_usd, 4),
                "roi_pct": round(roi_pct, 2),
            }

        except Exception as e:
            logger.debug(f"[BTC P&L] Error checking {pos.condition_id[:12]}: {e}")
            return None

    # ── CSV ─────────────────────────────────────────────────────────────────────

    def _write_result(self, pos: OpenPosition, result: dict):
        mode = "DRY" if pos.dry_run else "LIVE"
        profit_str = f"${result['profit_usd']:+.4f}"
        logger.info(
            f"[BTC P&L {mode}] {pos.question[:45]} | "
            f"{pos.side} @ {pos.entry_price:.3f} → {result['exit_price']:.3f} | "
            f"{result['outcome']} | {profit_str} ({result['roi_pct']:+.1f}%)"
        )

        row = {
            "entered_at":   pos.entered_at,
            "resolved_at":  result["resolved_at"],
            "condition_id": pos.condition_id,
            "question":     pos.question,
            "side":         pos.side,
            "entry_price":  pos.entry_price,
            "fair_value":   pos.fair_value,
            "edge":         pos.edge,
            "size_usd":     pos.size_usd,
            "dry_run":      pos.dry_run,
            "outcome":      result["outcome"],
            "exit_price":   result["exit_price"],
            "profit_usd":   result["profit_usd"],
            "roi_pct":      result["roi_pct"],
        }
        with open(self.csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
            writer.writerow(row)

    def _ensure_csv(self):
        if not self.csv_path.exists():
            self.csv_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.csv_path, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=CSV_HEADERS).writeheader()
            logger.info(f"[BTC P&L] CSV creado: {self.csv_path}")
