# polyedge

Polymarket arbitrage and edge scanner. Continuously monitors prediction markets, detects mispricings using multiple signal strategies, and optionally executes trades automatically.

## Signals

| Signal | Strategy | Academic basis |
|---|---|---|
| `PARITY` | YES + NO < $1.00 → buy both | arXiv 2508.03474 — $40M extracted on Polymarket |
| `MISPRICED_CORR` | Mutually exclusive outcomes don't sum to 1 | Same paper — Dutch book theorem |
| `SPREAD_CAPTURE` | Wide bid-ask vs low volatility | Market microstructure theory |
| `LONGSHOT_FADE` | Longshot bias — overpriced low-prob outcomes | Thaler (1985), extensive literature |
| `PRICE_DRIFT` | Price momentum across scan cycles | Lo & MacKinlay (1988) |
| `ODDS_DIVERGENCE` | Polymarket vs bookmaker odds (The Odds API) | Cross-market efficiency |
| `RESOLUTION_LAG` | Live score → win already certain, price lags | 5-10 min window documented empirically |
| `ORDER_BOOK_IMBALANCE` | Imbalance ratio predicts short-term price | R²=65% (Cao, Chen & Griffin 2005) |
| `WHALE` | On-chain large wallet movements (Alchemy) | Order flow toxicity (Glosten-Milgrom) |
| `NEWS_SENTIMENT` | News articles → sentiment shift (Finnhub) | Event studies literature |
| `MIROFISH` | Multi-agent LLM social simulation | OASIS (arXiv 2411.11581) |

## Setup

### Requirements

- Python 3.11+
- A Polymarket account with USDC deposited on Polygon
- API keys in `.env` (see below)

### Local install

```bash
git clone https://github.com/nachoilero7/bot-arbitraje.git
cd bot-arbitraje
python -m venv .venv
source .venv/bin/activate        # Linux/Mac
.venv\Scripts\activate           # Windows
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your keys
python main.py
```

### Run modes

```bash
python main.py                   # Continuous scanner (default)
python main.py --once            # Single scan and exit
python main.py --interval 10     # Override interval (seconds)
python main.py --mode btc        # BTC Up/Down daily monitor only
python main.py --mode both       # Scanner + BTC monitor in parallel
python main.py --dry-run         # Force simulation mode (no real trades)
```

## Configuration

Copy `.env.example` to `.env` and fill in your credentials:

```env
# Required for market scanning (no auth needed)
# --- nothing required ---

# Required for trade execution
POLYGON_PRIVATE_KEY=<your wallet private key>
POLYGON_PROXY_ADDRESS=<Gnosis Safe / Polymarket proxy wallet>
CLOB_API_KEY=<from polymarket.com/profile>
CLOB_API_SECRET=<from polymarket.com/profile>
CLOB_API_PASSPHRASE=<from polymarket.com/profile>

# Risk management
DRY_RUN=true                     # true = simulate only, false = real trades
BANKROLL_USD=100
MAX_POSITION_USD=20
MAX_DAILY_LOSS_USD=10
MIN_EDGE_TO_TRADE=0.04           # 4% minimum edge to execute

# Optional enrichers
ODDS_API_KEY=<the-odds-api.com>          # Sports odds divergence
RAPIDAPI_KEY=<rapidapi.com>              # Live scores (Resolution Lag)
FINNHUB_API_KEY=<finnhub.io>             # News sentiment
ALCHEMY_API_KEY=<alchemy.com>            # On-chain whale tracking

# Telegram notifications
TELEGRAM_BOT_TOKEN=<BotFather token>
TELEGRAM_CHAT_ID=<your chat id>
TELEGRAM_MIN_EDGE=0.05

# Proxy (if Polymarket is blocked by ISP)
# HTTPS_PROXY=socks5://127.0.0.1:1080
```

## Project structure

```
polyedge/
├── main.py                      # Entry point
├── config/
│   └── settings.yaml            # Scanner and signal config
├── src/
│   ├── api/
│   │   └── clob_client.py       # Gamma API + CLOB API clients
│   ├── signals/
│   │   ├── base.py              # BaseSignal, Opportunity dataclass
│   │   ├── parity.py            # YES+NO parity arb
│   │   ├── spread.py            # Spread capture
│   │   ├── longshot_fade.py     # Longshot bias
│   │   ├── price_drift.py       # Price momentum
│   │   ├── combinatorial_arb.py # Dutch book / MISPRICED_CORR
│   │   ├── odds_divergence.py   # Bookmaker divergence
│   │   ├── resolution_lag.py    # Live score lag
│   │   ├── orderbook_imbalance.py
│   │   ├── whale_signal.py
│   │   ├── news_sentiment.py
│   │   └── mirofish.py          # LLM multi-agent
│   ├── enrichers/               # External data sources
│   ├── execution/
│   │   └── trade_executor.py    # Order placement + Kelly sizing
│   ├── monitors/
│   │   ├── btc_arb_monitor.py   # BTC Up/Down daily
│   │   └── mirofish_runner.py   # Background LLM runner
│   ├── tracking/
│   │   └── pnl_tracker.py       # P&L tracking vs market resolution
│   ├── scanner/
│   │   └── market_scanner.py    # Main scan loop
│   ├── sizing/
│   │   └── kelly.py             # Fractional Kelly position sizing
│   └── notifications/
│       └── telegram.py
├── data/                        # Runtime data (gitignored)
│   ├── opportunities.csv
│   ├── trades.csv
│   └── pnl_state.json
├── logs/                        # Log files (gitignored)
├── deploy/
│   ├── setup.sh                 # Oracle Cloud Ubuntu setup script
│   └── polyedge.service         # systemd service file
├── requirements.txt
└── Dockerfile
```

## Deploy to Oracle Cloud (free 24/7)

Oracle Cloud Free Tier includes an ARM Ampere A1 VM with 4 OCPUs and 24GB RAM — permanently free. No VPN needed (US servers have full Polymarket access).

```bash
# 1. Create VM on Oracle Cloud Console
#    Shape: VM.Standard.A1.Flex (ARM) — 2 OCPU, 12GB RAM
#    OS: Ubuntu 22.04 (aarch64)
#    Open ports 22 (SSH) in Security List

# 2. Upload setup script and run
scp deploy/setup.sh ubuntu@<VM_IP>:~/
ssh ubuntu@<VM_IP> "bash ~/setup.sh"

# 3. Upload your .env
scp .env ubuntu@<VM_IP>:~/polyedge/.env

# 4. Start the service
ssh ubuntu@<VM_IP> "sudo systemctl start polyedge && sudo journalctl -u polyedge -f"
```

See [deploy/setup.sh](deploy/setup.sh) for the full automated setup.

## Docker

```bash
docker build -t polyedge .
docker run -d --name polyedge --env-file .env polyedge
docker logs -f polyedge
```

## Risk warning

This bot executes real financial transactions on prediction markets. Always start with `DRY_RUN=true` and small amounts. Past signal performance does not guarantee future results. You can lose money.
