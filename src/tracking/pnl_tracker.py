"""
P&L Tracker — rastrea resultados de trades contra resolución real de mercados.

Funciona con dry_run y live:
  - Dry run: muestra P&L hipotético si el edge fuera real (para validar señales)
  - Live: rastrea P&L real contra resoluciones de Polymarket

Estado de cada trade:
  open   → mercado sin resolver, calcula mark-to-market
  won    → mercado resolvió a favor (precio salida = 1.0)
  lost   → mercado resolvió en contra (precio salida = 0.0)
"""
import csv
import json
import os
from collections import defaultdict
from datetime import datetime

import requests
from rich.console import Console
from rich.table import Table
from rich import box

from src.utils.logger import get_logger

logger = get_logger(__name__)
console = Console()

GAMMA_API = "https://gamma-api.polymarket.com"


class PnLTracker:

    def __init__(self, trades_csv: str = "data/trades.csv", proxy: str = None):
        self.trades_csv = trades_csv
        self.state_file = "data/pnl_state.json"
        self._proxies = {"https": proxy, "http": proxy} if proxy else None
        self._state: dict = self._load_state()   # condition_id → {status, exit_price, current_price}

    # ── Public API ─────────────────────────────────────────────────────────────

    def update(self):
        """Consulta Gamma API para actualizar estado de trades abiertos."""
        trades = self._read_trades()
        live = [t for t in trades if not t["dry_run"] and t["success"] and t["condition_id"]]
        if not live:
            return

        # Solo refrescar los que no están resueltos
        open_ids = list({
            t["condition_id"] for t in live
            if self._state.get(t["condition_id"], {}).get("status", "open") == "open"
        })
        if not open_ids:
            return

        logger.debug(f"[PNL] Actualizando {len(open_ids)} mercados abiertos...")
        for i in range(0, len(open_ids), 20):
            self._fetch_batch(open_ids[i:i + 20])
        self._save_state()

    def print_summary(self):
        """Imprime resumen de P&L en consola. Funciona en dry_run y live."""
        trades = self._read_trades()
        if not trades:
            logger.info("[PNL] Sin trades registrados todavía.")
            return

        live   = [t for t in trades if not t["dry_run"] and t["success"]]
        dry    = [t for t in trades if t["dry_run"]  and t["success"]]

        if live:
            self._print_live_summary(live)
        if dry:
            self._print_dry_summary(dry)

    # ── Internal ───────────────────────────────────────────────────────────────

    def _fetch_batch(self, condition_ids: list[str]):
        try:
            url = f"{GAMMA_API}/markets?condition_ids={','.join(condition_ids)}"
            resp = requests.get(url, proxies=self._proxies, timeout=10)
            resp.raise_for_status()
            markets = resp.json()
            for m in markets:
                cid = m.get("conditionId") or m.get("condition_id", "")
                if not cid:
                    continue
                resolved = m.get("resolved", False) or m.get("isResolved", False)
                if resolved:
                    prices = m.get("outcomePrices", [])
                    try:
                        yes_exit = float(prices[0])
                    except (IndexError, TypeError, ValueError):
                        yes_exit = 0.5
                    self._state[cid] = {
                        "status": "won" if yes_exit >= 0.99 else "lost",
                        "exit_price": yes_exit,
                    }
                else:
                    # Mark-to-market: usar el mejor precio disponible
                    try:
                        current = float(m.get("bestBid") or m.get("bestAsk") or 0.5)
                    except (TypeError, ValueError):
                        current = 0.5
                    entry = self._state.get(cid, {})
                    self._state[cid] = {
                        "status": "open",
                        "current_price": current,
                        "exit_price": entry.get("exit_price"),
                    }
        except Exception as e:
            logger.debug(f"[PNL] Error fetching batch: {e}")

    def _calc_pnl(self, trade: dict) -> tuple[float, str]:
        """Retorna (pnl_usd, status) para un trade."""
        cid    = trade["condition_id"]
        side   = trade["side"]
        price  = trade["price"]
        size   = trade["size_tokens"]
        cost   = trade["position_usd"]
        state  = self._state.get(cid, {})
        status = state.get("status", "open")

        if status in ("won", "lost"):
            exit_p = state.get("exit_price", 1.0 if status == "won" else 0.0)
            if side == "YES":
                pnl = (exit_p - price) * size
            elif side == "NO":
                # NO tokens: ganas si YES resuelve a 0
                pnl = ((1.0 - exit_p) - price) * size
            else:  # YES+NO parity
                # price = p_yes + p_no (costo total por par), exit siempre = 1.0
                pnl = (1.0 - price) * size
        elif status == "open":
            current = state.get("current_price", price)
            if side == "YES":
                pnl = (current - price) * size
            elif side == "NO":
                pnl = ((1.0 - current) - price) * size
            else:
                pnl = (1.0 - price) * size * 0  # parity: P&L realizado en resolución
        else:
            pnl = 0.0

        return pnl, status

    def _print_live_summary(self, trades: list[dict]):
        total_invested = sum(t["position_usd"] for t in trades)
        total_pnl = 0.0
        by_signal: dict = defaultdict(lambda: {"invested": 0, "pnl": 0, "won": 0, "lost": 0, "open": 0})
        wins = losses = open_count = 0

        for t in trades:
            pnl, status = self._calc_pnl(t)
            total_pnl += pnl
            sig = t["signal_type"] or "UNKNOWN"
            by_signal[sig]["invested"] += t["position_usd"]
            by_signal[sig]["pnl"]      += pnl
            by_signal[sig][status]     += 1
            if status == "won":
                wins += 1
            elif status == "lost":
                losses += 1
            else:
                open_count += 1

        resolved = wins + losses
        win_rate = wins / resolved if resolved > 0 else None
        roi = total_pnl / total_invested if total_invested > 0 else 0

        pnl_str = f"[green]+${total_pnl:.2f}[/green]" if total_pnl >= 0 else f"[red]-${abs(total_pnl):.2f}[/red]"
        roi_str = f"[green]+{roi:.1%}[/green]" if roi >= 0 else f"[red]{roi:.1%}[/red]"

        table = Table(title="[bold cyan]P&L — Trades Reales[/bold cyan]", box=box.ROUNDED, show_header=True)
        table.add_column("Métrica",  style="cyan", width=22)
        table.add_column("Valor",    style="bold")

        table.add_row("Capital invertido",   f"${total_invested:.2f}")
        table.add_row("P&L total",            pnl_str)
        table.add_row("ROI",                  roi_str)
        table.add_row("Ganados / Perdidos",   f"{wins} / {losses}")
        table.add_row("Abiertos",             str(open_count))
        table.add_row("Win rate",             f"{win_rate:.0%}" if win_rate is not None else "—")

        console.print(table)

        # Breakdown por señal
        if len(by_signal) > 1:
            sig_table = Table(title="Por señal (live)", box=box.SIMPLE, show_header=True)
            sig_table.add_column("Señal",     style="dim")
            sig_table.add_column("Invertido", justify="right")
            sig_table.add_column("P&L",       justify="right")
            sig_table.add_column("W/L/O",     justify="right")
            for sig, d in sorted(by_signal.items()):
                pnl_c = f"[green]+${d['pnl']:.2f}[/green]" if d["pnl"] >= 0 else f"[red]-${abs(d['pnl']):.2f}[/red]"
                sig_table.add_row(sig, f"${d['invested']:.2f}", pnl_c, f"{d['won']}/{d['lost']}/{d['open']}")
            console.print(sig_table)

    def _print_dry_summary(self, trades: list[dict]):
        """Para dry_run: muestra P&L hipotético basado en el edge declarado."""
        total_sim = sum(t["position_usd"] for t in trades)
        total_edge = sum(t["edge"] * t["size_tokens"] for t in trades)
        by_signal: dict = defaultdict(lambda: {"count": 0, "sim_usd": 0, "expected_pnl": 0, "avg_edge": []})

        for t in trades:
            sig = t["signal_type"] or "UNKNOWN"
            by_signal[sig]["count"]        += 1
            by_signal[sig]["sim_usd"]      += t["position_usd"]
            by_signal[sig]["expected_pnl"] += t["edge"] * t["size_tokens"]
            by_signal[sig]["avg_edge"].append(t["edge"])

        table = Table(title="[bold yellow]P&L Simulado (Dry Run)[/bold yellow]", box=box.ROUNDED)
        table.add_column("Señal",         style="yellow")
        table.add_column("Trades",        justify="right")
        table.add_column("Capital sim.",  justify="right")
        table.add_column("Edge esperado", justify="right")
        table.add_column("Avg edge",      justify="right")

        for sig, d in sorted(by_signal.items(), key=lambda x: -x[1]["expected_pnl"]):
            avg = sum(d["avg_edge"]) / len(d["avg_edge"]) if d["avg_edge"] else 0
            table.add_row(
                sig,
                str(d["count"]),
                f"${d['sim_usd']:.2f}",
                f"[green]+${d['expected_pnl']:.2f}[/green]",
                f"{avg:.1%}",
            )

        console.print(table)
        console.print(
            f"[dim]  Total simulado: ${total_sim:.2f} | "
            f"Edge esperado acumulado: [green]+${total_edge:.2f}[/green] | "
            f"Trades: {len(trades)}[/dim]"
        )
        console.print(
            "[dim]  Nota: P&L hipotético asume que el edge declarado es correcto. "
            "Validar con capital real para confirmar.[/dim]"
        )

    def _read_trades(self) -> list[dict]:
        if not os.path.exists(self.trades_csv):
            return []
        trades = []
        with open(self.trades_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    trades.append({
                        "executed_at":  row.get("executed_at", ""),
                        "dry_run":      row.get("dry_run", "True").strip().lower() == "true",
                        "success":      row.get("success", "False").strip().lower() == "true",
                        "signal_type":  row.get("signal_type", ""),
                        "condition_id": row.get("condition_id", ""),
                        "token_id":     row.get("token_id", ""),
                        "side":         row.get("side", "YES"),
                        "price":        float(row.get("price", 0) or 0),
                        "size_tokens":  float(row.get("size_tokens", 0) or 0),
                        "position_usd": float(row.get("position_usd", 0) or 0),
                        "edge":         float(row.get("edge", 0) or 0),
                    })
                except (ValueError, KeyError):
                    continue
        return trades

    def _load_state(self) -> dict:
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

    def _save_state(self):
        os.makedirs("data", exist_ok=True)
        with open(self.state_file, "w", encoding="utf-8") as f:
            json.dump(self._state, f, indent=2)
