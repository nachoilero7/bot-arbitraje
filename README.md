# polyedge

Polymarket edge scanner and automated trading bot. Continuously monitors prediction markets, detects mispricings using multiple signal strategies backed by academic research, and optionally executes trades automatically.

> **Start with `DRY_RUN=true`.** The bot will simulate everything — detected edges, trade sizing, P&L — without touching real money. Only switch to `DRY_RUN=false` once you've validated the signals match your expectations.

---

## How it works

Every N seconds (default: 5) the bot:

1. Fetches up to 5000 open markets from Polymarket's Gamma API
2. Filters by liquidity (`MIN_LIQUIDITY_USD`) and resolution horizon (`MAX_DAYS_TO_RESOLUTION`)
3. Runs each market through all active signals
4. For opportunities above `MIN_EDGE_THRESHOLD`, enriches with external data (bookmaker odds, live scores, sentiment, superforecasters)
5. Logs every opportunity to `data/opportunities.csv`
6. If `edge >= MIN_EDGE_TO_TRADE`, sizes the position with fractional Kelly and executes (or simulates)
7. Tracks P&L against market resolution via `data/pnl_state.json`

---

## Signals

| Signal | Strategy | Basis |
|---|---|---|
| `PARITY` | YES + NO < $1.00 after fees → buy both, lock profit | arXiv 2508.03474 — $40M extracted on Polymarket |
| `MISPRICED_CORR` | Mutually exclusive outcomes don't sum to 1 (Dutch book) | Same paper — Dutch book theorem |
| `MULTI_OUTCOME_PARITY` | Sum of all YES prices in exclusive group < 1 − N×fees | Extension of parity to N-candidate races |
| `SPREAD_CAPTURE` | Wide bid-ask vs low volatility → NO farming | Market microstructure theory |
| `LONGSHOT_FADE` | Longshot bias — overpriced extremes (<12% or >82%) | Thaler (1985), behavioral economics |
| `PRICE_DRIFT` | Price momentum across scan cycles | Lo & MacKinlay (1988) |
| `CALIBRATION_BIAS` | YES < 10% occur 14% of the time (systematic miscalibration) | SSRN 5910522 — 124M Polymarket trades |
| `ODDS_DIVERGENCE` | Polymarket vs bookmaker consensus (The Odds API) | Cross-market efficiency |
| `RESOLUTION_LAG` | Live score already determines winner, price hasn't moved | 5–10 min window documented empirically |
| `ORDER_BOOK_IMBALANCE` | CLOB depth imbalance predicts short-term price direction | Cao, Chen & Griffin (2005) — R²=65% |
| `WHALE` | On-chain large wallet movements signal informed flow | Glosten-Milgrom order flow toxicity |
| `NEWS_SENTIMENT` | News articles → sentiment shift before market moves | Event studies literature |
| `MANIFOLD_DIV` | Polymarket vs Manifold Markets probability divergence | Dillon et al. (2023) — 63% accuracy >8pp |
| `METACULUS_DIV` | Polymarket vs Metaculus superforecasters divergence | Same paper — superforecasters outperform markets |
| `KALSHI_ARB` | Cross-market arbitrage between Polymarket and Kalshi | Regulatory-driven price separation |

Signals are additive: when multiple fire on the same market, edge scores combine and the opportunity ranks higher.

---

## Setup

### Requirements

- Python 3.11+
- A Polymarket account with USDC deposited on Polygon (needed only for live trading)

### Install

```bash
git clone https://github.com/nachoilero7/bot-arbitraje.git
cd bot-arbitraje
python -m venv .venv

# Linux/Mac
source .venv/bin/activate

# Windows
.venv\Scripts\activate

pip install -r requirements.txt
cp .env.example .env
```

---

## Running in dry-run mode (recommended first step)

Dry-run mode simulates everything — trades, sizing, P&L — without executing real transactions. You don't need Polymarket API keys for this.

**1. Minimal `.env` for dry-run:**

```env
DRY_RUN=true
BANKROLL_USD=100
MAX_POSITION_USD=2.50
MAX_DAILY_LOSS_USD=15
MIN_EDGE_TO_TRADE=0.04

SCAN_INTERVAL_SECONDS=5
MIN_EDGE_THRESHOLD=0.03
MIN_LIQUIDITY_USD=100
FEE_RATE=0.02
MAX_DAYS_TO_RESOLUTION=7
```

**2. Run:**

```bash
python main.py
```

**3. What to look at:**

- Terminal table: detected opportunities with signal type, edge %, side, and market question
- `data/opportunities.csv`: full history of every detected edge
- `data/trades.csv`: simulated trades with Kelly-sized position, entry price, and edge

Let it run for a few hours and review whether the signals make sense before going live.

---

## Running in live mode

**1. Get your Polymarket API credentials**

- Go to [polymarket.com](https://polymarket.com) → Profile → API Keys → Create key
- You'll get: `CLOB_API_KEY`, `CLOB_API_SECRET`, `CLOB_API_PASSPHRASE`
- Export your Polygon wallet private key from MetaMask (Settings → Security → Export private key)
- Find your Gnosis Safe proxy address: it appears in the Polymarket UI under your wallet address

**2. Fill in `.env`:**

```env
# Polymarket execution
POLYGON_PRIVATE_KEY=<your wallet private key>
POLYGON_PROXY_ADDRESS=<Gnosis Safe address from Polymarket>
CLOB_API_KEY=<from polymarket.com/profile>
CLOB_API_SECRET=<from polymarket.com/profile>
CLOB_API_PASSPHRASE=<from polymarket.com/profile>

# Risk limits
DRY_RUN=false
BANKROLL_USD=100          # Total capital to size against
MAX_POSITION_USD=2.50     # Max $ per trade
MAX_DAILY_LOSS_USD=15     # Kill switch: stops bot if daily loss exceeds this
MIN_EDGE_TO_TRADE=0.04    # Only trade when edge >= 4%
```

**3. Run:**

```bash
python main.py
```

The bot stops automatically if `MAX_DAILY_LOSS_USD` is hit. Restart the next day.

---

## Configuration reference

### Risk management

| Variable | Default | Description |
|---|---|---|
| `DRY_RUN` | `true` | `true` = simulate, `false` = real trades |
| `BANKROLL_USD` | `100` | Total capital for Kelly sizing |
| `MAX_POSITION_USD` | `2.50` | Hard cap per trade |
| `MAX_DAILY_LOSS_USD` | `15` | Daily kill switch |
| `MIN_EDGE_TO_TRADE` | `0.04` | Minimum edge to execute (4%) |

### Scanner

| Variable | Default | Description |
|---|---|---|
| `SCAN_INTERVAL_SECONDS` | `5` | Seconds between full market scans |
| `MIN_EDGE_THRESHOLD` | `0.03` | Minimum edge to log an opportunity (3%) |
| `MIN_LIQUIDITY_USD` | `100` | Skip markets below this liquidity |
| `FEE_RATE` | `0.02` | Polymarket taker fee (2%) |
| `MAX_DAYS_TO_RESOLUTION` | `7` | Skip markets resolving more than N days out |

### Optional enrichers

Each enricher activates an additional signal. All are free tier.

| Variable | Service | What it unlocks | Free limit |
|---|---|---|---|
| `ODDS_API_KEY` | [the-odds-api.com](https://the-odds-api.com) | `ODDS_DIVERGENCE` — bookmaker consensus for sports | 500 req/month |
| `RAPIDAPI_KEY` | [rapidapi.com](https://rapidapi.com/fluis.lacasse/api/allsportsapi2/) | `RESOLUTION_LAG` — live scores | 100 req/day |
| `FINNHUB_API_KEY` | [finnhub.io](https://finnhub.io) | `NEWS_SENTIMENT` — news events | 60 req/min |
| `ALCHEMY_API_KEY` | [alchemy.com](https://alchemy.com) | `WHALE` — on-chain wallet tracking | Free tier |
| `METACULUS_API_TOKEN` | [metaculus.com](https://www.metaculus.com) → Settings → API | `METACULUS_DIV` — superforecasters | Free account |

`MANIFOLD_DIV` is always active — Manifold Markets has a public API that requires no key.

To get your Metaculus token: register at metaculus.com → Settings → API → copy token.

### Telegram notifications

```env
TELEGRAM_BOT_TOKEN=<token from @BotFather>
TELEGRAM_CHAT_ID=<your chat id>
TELEGRAM_MIN_EDGE=0.05   # Only notify when edge >= 5%
```

To find your chat ID: send `/start` to your bot, then open `https://api.telegram.org/bot{TOKEN}/getUpdates` and copy the `id` field from `chat`.

### Proxy

If Polymarket is blocked by your ISP (common outside US/EU):

```env
HTTPS_PROXY=socks5://127.0.0.1:1080   # SOCKS5 (Clash, V2Ray, etc.)
# or
HTTPS_PROXY=http://127.0.0.1:8080     # HTTP proxy
```

---

## Run modes

```bash
python main.py                    # Continuous scanner (default)
python main.py --once             # Single scan and exit
python main.py --interval 10      # Override scan interval (seconds)
python main.py --mode btc         # BTC Up/Down daily monitor only
python main.py --mode both        # Scanner + BTC monitor in parallel
python main.py --dry-run          # Force simulation (overrides DRY_RUN=false in .env)
```

---

## Output files

All files are in `data/` (gitignored):

| File | Contents |
|---|---|
| `opportunities.csv` | Every detected edge: signal, market, side, edge %, liquidity, notes |
| `trades.csv` | Every executed (or simulated) trade: price, size, Kelly fraction, order ID |
| `pnl_state.json` | Live P&L state per position: status (open/won/lost), current price, CLV |

P&L summary is printed to console every 10 scans and shows: realized P&L, unrealized mark-to-market, win rate, and average CLV (Closing Line Value — positive CLV confirms real edge, not luck).

---

## Project structure

```
polyedge/
├── main.py                          # Entry point + CLI
├── config/
│   └── settings.yaml                # Signal thresholds, scanner config
├── src/
│   ├── api/
│   │   └── clob_client.py           # Gamma API + CLOB API clients
│   ├── signals/
│   │   ├── base.py                  # BaseSignal, Opportunity dataclass
│   │   ├── parity.py                # YES+NO parity + multi-outcome parity
│   │   ├── combinatorial_arb.py     # Dutch book / MISPRICED_CORR
│   │   ├── spread.py                # Spread capture / NO farming
│   │   ├── longshot_fade.py         # Longshot bias fade
│   │   ├── price_drift.py           # Price momentum
│   │   ├── calibration_bias.py      # Systematic miscalibration signal
│   │   ├── odds_divergence.py       # Bookmaker vs Polymarket
│   │   ├── resolution_lag.py        # Live score lag
│   │   ├── orderbook_imbalance.py   # CLOB depth imbalance
│   │   ├── whale_signal.py          # On-chain flow
│   │   ├── news_sentiment.py        # News events
│   │   └── metaculus_divergence.py  # Cross-platform divergence (Manifold + Metaculus)
│   ├── enrichers/
│   │   ├── odds_api.py              # The Odds API
│   │   ├── allsports.py             # RapidAPI live scores
│   │   ├── finnhub.py               # Finnhub news
│   │   ├── alchemy.py               # On-chain data
│   │   ├── manifold.py              # Manifold Markets (no key needed)
│   │   └── metaculus.py             # Metaculus superforecasters
│   ├── execution/
│   │   └── trade_executor.py        # Order placement + kill switch
│   ├── sizing/
│   │   └── kelly.py                 # Fractional Kelly position sizing
│   ├── monitors/
│   │   └── btc_arb_monitor.py       # BTC Up/Down daily monitor
│   ├── tracking/
│   │   └── pnl_tracker.py           # P&L vs market resolution + CLV
│   ├── scanner/
│   │   └── market_scanner.py        # Main scan loop
│   └── notifications/
│       └── telegram.py
├── data/                            # Runtime data (gitignored)
├── logs/                            # Daily log files (gitignored)
├── deploy/
│   ├── setup.sh                     # Oracle Cloud Ubuntu setup script
│   └── polyedge.service             # systemd service file
├── requirements.txt
└── Dockerfile
```

---

## Deploy to Oracle Cloud (free, 24/7)

Oracle Cloud Free Tier includes an ARM VM (4 OCPUs, 24GB RAM) — permanently free. US-based servers have full Polymarket access without VPN.

```bash
# 1. Create VM on Oracle Cloud Console
#    Shape: VM.Standard.A1.Flex — 2 OCPU, 12GB RAM
#    OS: Ubuntu 22.04 (aarch64)

# 2. Run automated setup
scp deploy/setup.sh ubuntu@<VM_IP>:~/
ssh ubuntu@<VM_IP> "bash ~/setup.sh"

# 3. Upload your .env
scp .env ubuntu@<VM_IP>:~/polyedge/.env

# 4. Start as a system service
ssh ubuntu@<VM_IP> "sudo systemctl enable --now polyedge"
ssh ubuntu@<VM_IP> "sudo journalctl -u polyedge -f"
```

## Docker

```bash
docker build -t polyedge .
docker run -d --name polyedge --env-file .env polyedge
docker logs -f polyedge
```

---

## Risk warning

This bot executes real financial transactions on prediction markets. Always start with `DRY_RUN=true`. Review `data/opportunities.csv` and `data/trades.csv` before enabling live mode. Past signal performance does not guarantee future results. You can lose money. Size conservatively — the default `MAX_POSITION_USD=2.50` exists for a reason.
