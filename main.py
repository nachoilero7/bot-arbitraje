"""
polyedge - Polymarket Edge Scanner

Modos:
    python main.py                    # Scanner general continuo
    python main.py --once             # Un solo scan y salir
    python main.py --interval 10      # Override del intervalo en segundos
    python main.py --mode btc         # Solo monitor BTC Up or Down diario
    python main.py --mode both        # Scanner general + BTC monitor en paralelo
"""
import argparse
import time
import signal
import sys
import os

import yaml
from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel

from src.api.clob_client import GammaApiClient
from src.scanner.market_scanner import MarketScanner
from src.utils.logger import get_logger

load_dotenv()
console = Console()
logger  = get_logger("polyedge")


def load_config(path: str = "config/settings.yaml") -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def main():
    parser = argparse.ArgumentParser(description="polyedge - Polymarket Edge Scanner")
    parser.add_argument("--once",     action="store_true", help="Ejecutar un solo scan y salir")
    parser.add_argument("--interval", type=int, default=None, help="Intervalo en segundos entre scans")
    parser.add_argument(
        "--mode",
        choices=["scanner", "btc", "both"],
        default="scanner",
        help="Modo de operacion: scanner (default), btc (BTC Up/Down diario), both (ambos en paralelo)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Forzar modo simulacion (override DRY_RUN=false en .env)")
    args = parser.parse_args()

    cfg = load_config()
    api_cfg     = cfg.get("api", {})
    scanner_cfg = cfg.get("scanner", {})
    signals_cfg = cfg.get("signals", {})
    logging_cfg = cfg.get("logging", {})

    interval = args.interval or int(os.getenv("SCAN_INTERVAL_SECONDS", scanner_cfg.get("interval_seconds", 30)))
    min_edge = float(os.getenv("MIN_EDGE_THRESHOLD", signals_cfg.get("min_edge_threshold", 0.03)))

    mode_label = {
        "scanner": "Scanner General",
        "btc":     "BTC Up/Down Monitor",
        "both":    "Scanner General + BTC Monitor",
    }[args.mode]

    console.print(Panel.fit(
        "[bold cyan]polyedge[/bold cyan] [dim]— Polymarket Edge Scanner[/dim]\n"
        f"[dim]Modo: {mode_label} | Interval: {interval}s | Min edge: {min_edge:.0%}[/dim]",
        border_style="cyan"
    ))

    # Credenciales compartidas
    proxy               = os.getenv("HTTPS_PROXY") or os.getenv("HTTP_PROXY") or None
    odds_api_key        = os.getenv("ODDS_API_KEY") or None
    rapidapi_key        = os.getenv("RAPIDAPI_KEY") or None
    finnhub_api_key       = os.getenv("FINNHUB_API_KEY") or None
    metaculus_api_token   = os.getenv("METACULUS_API_TOKEN") or None
    telegram_token      = os.getenv("TELEGRAM_BOT_TOKEN") or None
    telegram_chat_id    = os.getenv("TELEGRAM_CHAT_ID") or None
    clob_private_key    = os.getenv("POLYGON_PRIVATE_KEY") or None
    clob_api_key        = os.getenv("CLOB_API_KEY") or None
    clob_api_secret     = os.getenv("CLOB_API_SECRET") or None
    clob_api_passphrase = os.getenv("CLOB_API_PASSPHRASE") or None
    alchemy_api_key     = os.getenv("ALCHEMY_API_KEY") or None
    polygon_proxy_addr  = os.getenv("POLYGON_PROXY_ADDRESS") or None
    dry_run             = args.dry_run or (os.getenv("DRY_RUN", "true").lower() != "false")
    bankroll_usd        = float(os.getenv("BANKROLL_USD", "100"))
    max_position_usd    = float(os.getenv("MAX_POSITION_USD", "20"))
    max_daily_loss_usd  = float(os.getenv("MAX_DAILY_LOSS_USD", "10"))
    min_edge_to_trade       = float(os.getenv("MIN_EDGE_TO_TRADE", "0.06"))
    max_days_to_resolution  = int(os.getenv("MAX_DAYS_TO_RESOLUTION", "7"))

    running = True
    def shutdown(sig, frame):
        nonlocal running
        console.print("\n[yellow]Shutting down...[/yellow]")
        running = False
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # ── BTC Monitor ────────────────────────────────────────────────────────────
    btc_monitor = None
    if args.mode in ("btc", "both"):
        from src.monitors.btc_arb_monitor import BtcArbMonitor

        # Construir notifier y executor standalone para el monitor BTC
        notifier = None
        executor = None

        try:
            from src.notifications.telegram import TelegramNotifier
            if telegram_token and telegram_chat_id:
                notifier = TelegramNotifier(
                    bot_token=telegram_token,
                    chat_id=telegram_chat_id,
                    min_notify_edge=float(os.getenv("TELEGRAM_MIN_EDGE", "0.05")),
                )
        except ImportError:
            pass

        try:
            from src.execution.trade_executor import TradeExecutor
            if clob_private_key and clob_api_key:
                executor = TradeExecutor(
                    private_key=clob_private_key,
                    api_key=clob_api_key,
                    api_secret=clob_api_secret,
                    api_passphrase=clob_api_passphrase,
                    bankroll_usd=bankroll_usd,
                    max_position_usd=max_position_usd,
                    max_daily_loss_usd=max_daily_loss_usd,
                    max_days_to_resolution=max_days_to_resolution,
                    dry_run=dry_run,
                    proxy_address=polygon_proxy_addr,
                )
        except ImportError:
            pass

        btc_monitor = BtcArbMonitor(
            executor=executor,
            notifier=notifier,
            min_edge=float(os.getenv("MIN_EDGE_BTC", "0.09")),
            dry_run=dry_run,
            bankroll_usd=bankroll_usd,
        )
        btc_monitor.start()

        mode_str = "DRY RUN" if dry_run else "LIVE"
        logger.info(f"[BTC ARB] Monitor diario iniciado [{mode_str}]")

    # ── P&L Tracker ────────────────────────────────────────────────────────────
    pnl_tracker = None
    try:
        from src.tracking.pnl_tracker import PnLTracker
        pnl_tracker = PnLTracker(trades_csv="data/trades.csv", proxy=proxy)
        pnl_tracker.update()
        pnl_tracker.print_summary()
    except Exception as e:
        logger.warning(f"[PNL] Tracker no disponible: {e}")

    # ── Scanner General ────────────────────────────────────────────────────────
    if args.mode in ("scanner", "both"):
        client = GammaApiClient(
            timeout=api_cfg.get("request_timeout", 15),
            max_retries=api_cfg.get("max_retries", 3),
            proxy=proxy,
        )
        scanner = MarketScanner(
            client=client,
            fee_rate=float(os.getenv("FEE_RATE", signals_cfg.get("fee_rate", 0.02))),
            min_edge=min_edge,
            min_liquidity_usd=float(os.getenv("MIN_LIQUIDITY_USD", scanner_cfg.get("min_liquidity_usd", 500))),
            max_markets=scanner_cfg.get("max_markets", 5000),
            opportunities_csv=logging_cfg.get("opportunities_csv", "data/opportunities.csv"),
            odds_api_key=odds_api_key,
            rapidapi_key=rapidapi_key,
            finnhub_api_key=finnhub_api_key,
            metaculus_api_token=metaculus_api_token,
            telegram_token=telegram_token,
            telegram_chat_id=telegram_chat_id,
            telegram_min_edge=float(os.getenv("TELEGRAM_MIN_EDGE", "0.05")),
            proxy=proxy,
            alchemy_api_key=alchemy_api_key,
            clob_private_key=clob_private_key,
            clob_api_key=clob_api_key,
            clob_api_secret=clob_api_secret,
            clob_api_passphrase=clob_api_passphrase,
            bankroll_usd=bankroll_usd,
            max_position_usd=max_position_usd,
            max_daily_loss_usd=max_daily_loss_usd,
            min_edge_to_trade=min_edge_to_trade,
            max_days_to_resolution=max_days_to_resolution,
            dry_run=dry_run,
            polygon_proxy_address=polygon_proxy_addr,
        )

        scan_num = 0
        while running:
            scan_num += 1
            scanner.scan_once(scan_num=scan_num)

            # Actualizar P&L cada 10 scans e imprimir resumen
            if pnl_tracker and scan_num % 10 == 0:
                try:
                    pnl_tracker.update()
                    pnl_tracker.print_summary()
                except Exception as e:
                    logger.debug(f"[PNL] update error: {e}")

            if args.once:
                break

            if running:
                for _ in range(interval):
                    if not running:
                        break
                    time.sleep(1)

    elif args.mode == "btc":
        # Solo BTC monitor — el main thread duerme mientras el monitor corre en background
        logger.info("[BTC ARB] Corriendo en modo BTC-only. Ctrl+C para detener.")
        while running:
            time.sleep(1)

    if btc_monitor:
        btc_monitor.stop()

    console.print("[dim]polyedge stopped.[/dim]")


if __name__ == "__main__":
    main()
