"""
Market Scanner - Loop principal.

Cada N segundos:
1. Fetch mercados activos desde Gamma API (ya incluye precios bid/ask/spread)
2. Filtra mercados zombie (sin volumen reciente)
3. Corre todos los signal detectors
4. Muestra tabla Rich + guarda CSV + notifica Telegram
"""
import csv
import os
from datetime import datetime, timezone, timedelta

from rich.console import Console
from rich.table import Table
from rich import box

from src.api.clob_client import GammaApiClient, ClobApiClient
from src.signals.base import BaseSignal, Opportunity
from src.signals.parity import ParitySignal
from src.signals.spread import SpreadSignal
from src.signals.longshot_fade import LongshotFadeSignal
from src.signals.price_drift import PriceDriftSignal
from src.signals.combinatorial_arb import CombinatorialArbSignal
from src.signals.calibration_bias import CalibrationBiasSignal
from src.utils.logger import get_logger

# Enrichers opcionales
try:
    from src.enrichers.odds_api import OddsApiClient
    from src.signals.odds_divergence import OddsDivergenceSignal
    _ODDS_AVAILABLE = True
except ImportError:
    _ODDS_AVAILABLE = False

try:
    from src.enrichers.kalshi import KalshiApiClient
    from src.signals.kalshi_arb import KalshiArbSignal
    _KALSHI_AVAILABLE = True
except ImportError:
    _KALSHI_AVAILABLE = False

try:
    from src.signals.resolution_lag import ResolutionLagSignal
    _RESOLUTION_AVAILABLE = True
except ImportError:
    _RESOLUTION_AVAILABLE = False

try:
    from src.signals.orderbook_imbalance import OrderBookImbalanceSignal
    from src.enrichers.whale_tracker import WhaleTracker
    from src.signals.whale_signal import WhaleSignal
    _CLOB_SIGNALS_AVAILABLE = True
except ImportError:
    _CLOB_SIGNALS_AVAILABLE = False

try:
    from src.enrichers.alchemy_whale import AlchemyWhaleTracker
    _ALCHEMY_AVAILABLE = True
except ImportError:
    _ALCHEMY_AVAILABLE = False

try:
    from src.enrichers.finnhub import FinnhubClient
    from src.signals.news_sentiment import NewsSentimentSignal
    _FINNHUB_AVAILABLE = True
except ImportError:
    _FINNHUB_AVAILABLE = False

try:
    from src.notifications.telegram import TelegramNotifier
    _TELEGRAM_AVAILABLE = True
except ImportError:
    _TELEGRAM_AVAILABLE = False

try:
    from src.execution.trade_executor import TradeExecutor
    _EXECUTOR_AVAILABLE = True
except ImportError:
    _EXECUTOR_AVAILABLE = False


logger = get_logger(__name__)
console = Console(width=160)

# Filtro anti-zombie: mercados con muy poco volumen reciente son ruido
MIN_VOLUME_FOR_SPREAD = 10.0   # $10 vol 24h minimo para SPREAD_CAPTURE

# Filtro de horizonte temporal: ignorar mercados que resuelven muy lejos en el futuro
MAX_DAYS_TO_RESOLUTION = 30    # solo mercados que resuelven en los proximos 30 dias


class MarketScanner:

    def __init__(
        self,
        client: GammaApiClient,
        fee_rate: float = 0.02,
        min_edge: float = 0.03,
        min_liquidity_usd: float = 500,
        max_markets: int = 5000,
        opportunities_csv: str = "data/opportunities.csv",
        odds_api_key: str = None,
        rapidapi_key: str = None,
        finnhub_api_key: str = None,
        telegram_token: str = None,
        telegram_chat_id: str = None,
        telegram_min_edge: float = 0.05,
        proxy: str = None,
        alchemy_api_key: str = None,
        # Phase 2 — trade execution
        clob_private_key: str = None,
        clob_api_key: str = None,
        clob_api_secret: str = None,
        clob_api_passphrase: str = None,
        bankroll_usd: float = 100.0,
        max_position_usd: float = 20.0,
        max_daily_loss_usd: float = 10.0,
        min_edge_to_trade: float = 0.06,
        max_days_to_resolution: int = 7,
        dry_run: bool = True,
        polygon_proxy_address: str = None,  # Privy proxy wallet (POLY_PROXY mode)
    ):
        self.client = client
        self.min_liquidity_usd = min_liquidity_usd
        self.max_markets = max_markets
        self.opportunities_csv = opportunities_csv
        self.notifier = None

        self.signals: list[BaseSignal] = [
            ParitySignal(fee_rate=fee_rate, min_edge=min_edge),
            SpreadSignal(fee_rate=fee_rate, min_edge=min_edge),
            LongshotFadeSignal(fee_rate=fee_rate, min_edge=min_edge),
            PriceDriftSignal(fee_rate=fee_rate, min_edge=min_edge),
            CombinatorialArbSignal(fee_rate=fee_rate, min_edge=min_edge),
            CalibrationBiasSignal(fee_rate=fee_rate, min_edge=min_edge),
        ]
        logger.info("LongshotFadeSignal enabled")
        logger.info("PriceDriftSignal enabled")
        logger.info("CombinatorialArbSignal enabled (mutual exclusion + multi-outcome parity)")
        logger.info("CalibrationBiasSignal enabled (SSRN 5910522, 124M trades)")

        # The Odds API (H2H — free tier)
        if odds_api_key and _ODDS_AVAILABLE:
            odds_client = OddsApiClient(api_key=odds_api_key)
            self.signals.append(OddsDivergenceSignal(
                odds_client=odds_client,
                fee_rate=fee_rate,
                min_edge=min_edge,
            ))
            logger.info("OddsDivergenceSignal enabled (The Odds API)")

        # Kalshi cross-market arbitrage
        if _KALSHI_AVAILABLE:
            kalshi_client = KalshiApiClient()
            self.signals.append(KalshiArbSignal(
                kalshi_client=kalshi_client,
                fee_rate=fee_rate,
                min_edge=min_edge,
            ))
            logger.info("KalshiArbSignal enabled")

        # Resolution Lag (live scores via RapidAPI)
        if rapidapi_key and _RESOLUTION_AVAILABLE:
            self.signals.append(ResolutionLagSignal(
                rapidapi_key=rapidapi_key,
                fee_rate=fee_rate,
                min_edge=min_edge,
            ))
            logger.info("ResolutionLagSignal enabled (AllSportsAPI2)")

        # CLOB-based signals: OrderBook Imbalance
        if _CLOB_SIGNALS_AVAILABLE:
            clob_client = ClobApiClient(proxy=proxy)
            self.signals.append(OrderBookImbalanceSignal(
                clob_client=clob_client,
                fee_rate=fee_rate,
                min_edge=min_edge,
            ))
            logger.info("OrderBookImbalanceSignal enabled (CLOB API)")

        # On-chain whale tracking via Alchemy (Polygon)
        self._alchemy_whale: AlchemyWhaleTracker | None = None
        if alchemy_api_key and _ALCHEMY_AVAILABLE and _CLOB_SIGNALS_AVAILABLE:
            try:
                self._alchemy_whale = AlchemyWhaleTracker(api_key=alchemy_api_key)
                self._alchemy_whale.start()
                self.signals.append(WhaleSignal(
                    whale_tracker=self._alchemy_whale,
                    fee_rate=fee_rate,
                    min_edge=min_edge,
                ))
                logger.info("WhaleSignal enabled (Alchemy on-chain, Polygon)")
            except Exception as e:
                logger.warning(f"AlchemyWhaleTracker init failed: {e}")

        # Finnhub news sentiment
        if finnhub_api_key and _FINNHUB_AVAILABLE:
            finnhub_client = FinnhubClient(api_key=finnhub_api_key)
            self.signals.append(NewsSentimentSignal(
                finnhub_client=finnhub_client,
                fee_rate=fee_rate,
                min_edge=min_edge,
            ))
            logger.info("NewsSentimentSignal enabled (Finnhub)")

        # Telegram notifier
        if telegram_token and telegram_chat_id and _TELEGRAM_AVAILABLE:
            self.notifier = TelegramNotifier(
                bot_token=telegram_token,
                chat_id=telegram_chat_id,
                min_notify_edge=telegram_min_edge,
            )
            signal_names = [s.name for s in self.signals]
            self.notifier.send_startup(interval=30, signals=signal_names)
            logger.info("Telegram notifications enabled")

        # Trade executor (Phase 2)
        self.executor = None
        if clob_private_key and clob_api_key and _EXECUTOR_AVAILABLE:
            self.executor = TradeExecutor(
                private_key=clob_private_key,
                api_key=clob_api_key,
                api_secret=clob_api_secret,
                api_passphrase=clob_api_passphrase,
                bankroll_usd=bankroll_usd,
                max_position_usd=max_position_usd,
                max_daily_loss_usd=max_daily_loss_usd,
                min_edge_to_trade=min_edge_to_trade,
                max_days_to_resolution=max_days_to_resolution,
                dry_run=dry_run,
                proxy_address=polygon_proxy_address,
            )
            mode = "DRY RUN" if dry_run else "LIVE"
            logger.info(f"TradeExecutor enabled [{mode}]")

        self._ensure_csv()

    def _filter_markets(self, markets: list[dict]) -> list[dict]:
        """Filtra mercados zombie y mercados con horizonte de resolucion muy lejano."""
        now = datetime.now(timezone.utc)
        cutoff = now + timedelta(days=MAX_DAYS_TO_RESOLUTION)
        active = []
        skipped_horizon = 0
        for m in markets:
            # Filtro de horizonte temporal: descartar mercados que resuelven muy lejos
            end_date_str = m.get("endDateIso") or m.get("endDate") or ""
            if end_date_str:
                try:
                    # endDateIso es "YYYY-MM-DD", endDate es "YYYY-MM-DDTHH:MM:SSZ"
                    end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                    if end_dt.tzinfo is None:
                        end_dt = end_dt.replace(tzinfo=timezone.utc)
                    if end_dt > cutoff:
                        skipped_horizon += 1
                        continue
                except (ValueError, AttributeError):
                    pass  # si no podemos parsear la fecha, incluimos el mercado

            vol = m.get("volume24hr") or 0
            spread = m.get("spread") or 0
            # Mercados con spread amplio pero sin volumen = zombie, ignorar para spread signal
            # Los mantenemos igual para parity/divergence signals
            m["_is_active"] = vol >= MIN_VOLUME_FOR_SPREAD or spread < 0.1
            active.append(m)

        if skipped_horizon:
            logger.debug(f"[FILTER] Skipped {skipped_horizon} markets resolving after {cutoff.date()} (+{MAX_DAYS_TO_RESOLUTION}d)")
        return active

    def _ensure_csv(self):
        os.makedirs("data", exist_ok=True)
        if not os.path.exists(self.opportunities_csv):
            with open(self.opportunities_csv, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "detected_at", "signal_type", "category", "question",
                    "side", "market_price", "fair_value", "edge", "edge_pct",
                    "best_bid", "best_ask", "spread", "liquidity_usd", "volume_24h", "notes"
                ])

    def _run_signals(self, markets: list[dict]) -> list[Opportunity]:
        all_opps = []
        for signal in self.signals:
            try:
                opps = signal.detect(markets)
                if opps:
                    logger.info(f"[{signal.name}] {len(opps)} opportunities")
                all_opps.extend(opps)
            except Exception as e:
                logger.error(f"Signal {signal.name} failed: {e}")
        all_opps.sort(key=lambda o: o.edge, reverse=True)
        return all_opps

    def _print_opportunities(self, opportunities: list[Opportunity], scan_num: int, total_markets: int):
        timestamp = datetime.utcnow().strftime("%H:%M:%S")

        if not opportunities:
            console.print(
                f"[dim]Scan #{scan_num} ({timestamp} UTC) — {total_markets} markets scanned — "
                f"No opportunities above threshold[/dim]"
            )
            return

        table = Table(
            title=f"[bold green]Scan #{scan_num} — {timestamp} UTC — {total_markets} markets[/bold green]",
            box=box.ROUNDED,
            show_lines=True,
        )
        table.add_column("Signal",    style="cyan",       no_wrap=True, max_width=15)
        table.add_column("Question",  style="white",      max_width=52)
        table.add_column("Category",  style="magenta",    no_wrap=True, max_width=20)
        table.add_column("Side",      style="yellow",     no_wrap=True, justify="center")
        table.add_column("Price",     style="blue",       no_wrap=True, justify="right")
        table.add_column("Edge",      style="bold green", no_wrap=True, justify="right")
        table.add_column("Spread",    style="dim",        no_wrap=True, justify="right")
        table.add_column("Liq $",     style="green dim",  no_wrap=True, justify="right")
        table.add_column("Vol 24h $", style="dim",        no_wrap=True, justify="right")

        for opp in opportunities[:25]:
            edge_color = "bright_green" if opp.edge >= 0.06 else "green"
            liq = f"{opp.liquidity_usd:>8,.0f}" if opp.liquidity_usd else "    -"
            vol = f"{opp.volume_24h:>8,.0f}"    if opp.volume_24h    else "    -"
            table.add_row(
                opp.signal_type.value[:15],
                (opp.question or "-")[:52],
                (opp.category or "-")[:20],
                opp.side,
                f"{opp.market_price:.3f}",
                f"[{edge_color}]+{opp.edge:.4f}[/{edge_color}]",
                f"{opp.spread:.3f}",
                liq,
                vol,
            )

        console.print(table)

    def _save_opportunities(self, opportunities: list[Opportunity]):
        if not opportunities:
            return
        with open(self.opportunities_csv, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            for opp in opportunities:
                writer.writerow([
                    opp.detected_at.isoformat(),
                    opp.signal_type.value,
                    opp.category,
                    opp.question,
                    opp.side,
                    opp.market_price,
                    opp.fair_value,
                    opp.edge,
                    round(opp.edge_pct, 4),
                    opp.best_bid,
                    opp.best_ask,
                    opp.spread,
                    round(opp.liquidity_usd, 2),
                    round(opp.volume_24h, 2),
                    opp.notes,
                ])

    def scan_once(self, scan_num: int = 1) -> list[Opportunity]:
        try:
            markets = self.client.get_all_active_markets(
                min_liquidity=self.min_liquidity_usd,
                max_markets=self.max_markets,
            )
            markets = self._filter_markets(markets)
            opportunities = self._run_signals(markets)

            # Enriquecer oportunidades con end_date para filtro de horizonte en executor
            market_by_cid = {m.get("conditionId", ""): m for m in markets}
            for opp in opportunities:
                m = market_by_cid.get(opp.condition_id, {})
                opp.end_date = m.get("endDate", "") or m.get("endDateIso", "")

            self._print_opportunities(opportunities, scan_num, len(markets))
            self._save_opportunities(opportunities)

            if self.notifier and not self.executor:
                self.notifier.notify_opportunities(opportunities, scan_num)
                if scan_num % 10 == 0:
                    top = opportunities[0].edge if opportunities else 0
                    self.notifier.send_summary(len(opportunities), top, scan_num)

            # Phase 2: ejecutar trades en oportunidades de alta confianza
            if self.executor:
                for opp in opportunities:
                    result = self.executor.maybe_execute(opp)
                    if result and self.notifier:
                        mode = "DRY RUN" if result.dry_run else "TRADE"
                        msg = (
                            f"[{mode}] {opp.signal_type.value}\n"
                            f"{opp.question[:60]}\n"
                            f"Side: {opp.side} @ {result.price:.3f} | ${result.position_usd:.2f} | edge={opp.edge:.4f}"
                        )
                        if not result.dry_run:
                            msg += f"\nOrder ID: {result.order_id}"
                        self.notifier._send(msg)

            return opportunities
        except Exception as e:
            logger.error(f"Scan #{scan_num} failed: {e}", exc_info=True)
            return []
