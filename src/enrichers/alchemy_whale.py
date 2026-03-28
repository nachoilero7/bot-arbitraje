"""
Enricher: Alchemy On-Chain Whale Tracker (Polygon)
Rastrea grandes operadores directamente en la blockchain de Polygon
usando la API de Alchemy (sin web3 — JSON-RPC puro via requests).

Contratos monitoreados:
  CTF Exchange:      0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E
  Neg Risk Exchange: 0xC5d563A36AE78145C45a50134d48A1215220f80a

Evento:
  OrderFilled(bytes32 orderHash, address maker, address taker,
              uint256 makerAssetId, uint256 takerAssetId,
              uint256 makerAmountFilled, uint256 takerAmountFilled,
              uint256 fee)

Firma: keccak256("OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)")
  = 0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06de0ec9cd5c9f4fd0aae4f85

Logica de decodificacion:
  - Los topics[0] = event signature
  - topics[1] = orderHash (bytes32)
  - topics[2] = maker (address, padded)
  - topics[3] = taker (address, padded)
  - data = 5 * 32 bytes: [makerAssetId, takerAssetId, makerAmountFilled, takerAmountFilled, fee]

  USDC en Polygon: 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174 (USDC.e)
                   0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359 (native USDC)
  Como uint256, la address USDC tiene los 12 bytes superiores en cero.

  Si makerAssetId < 1e18 (address-space) → maker esta vendiendo USDC (BUY order)
  Si takerAssetId < 1e18                 → taker esta vendiendo USDC (SELL order para el token)

  makerAmountFilled / takerAmountFilled en USDC = size en dolares (6 decimales en Polygon USDC)

Interface compatible con WhaleTracker:
  get_whale_pressure(token_id: str) -> {buy_volume, sell_volume, ratio, whale_count}
"""
import os
import time
import threading
import requests
from collections import defaultdict
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Contratos Polymarket en Polygon
CTF_EXCHANGE      = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
NEG_RISK_EXCHANGE = "0xC5d563A36AE78145C45a50134d48A1215220f80a"

# Firma del evento OrderFilled (verificada on-chain, CTF Exchange Polygon)
ORDER_FILLED_TOPIC = "0xd0a08e8c493f9c94f29311604c9de1b4e8c8d4c06bd0c789af57f2d65bfec0f6"

# USDC.e en Polygon como uint256 (address con padding de ceros)
# La address tiene 20 bytes -> como uint256 es un numero < 2^160 pero << 2^256
# Umbral: cualquier assetId que sea una address ETH tipica (< 2^161) es USDC/stablecoin
USDC_ASSET_THRESHOLD = 2 ** 161   # cualquier token con id < esto es probablemente un ERC-20 address

# Tamano minimo para considerarse "whale" (en USDC, 6 decimales → $2000)
WHALE_USDC_THRESHOLD = 2_000 * 10**6   # 2000 USDC con 6 decimales

# TTL del cache de presion por token
PRESSURE_CACHE_TTL = 120  # segundos

# Ventana de tiempo para buscar eventos: ultimos N bloques
# Polygon ~2s/block → 10 bloques = ~20 segundos
# Free tier Alchemy: limite de 10 bloques por eth_getLogs
LOOKBACK_BLOCKS = 9


class AlchemyWhaleTracker:
    """
    Consulta eventos OrderFilled on-chain via Alchemy JSON-RPC
    y calcula presion de ballenas para cada token de Polymarket.

    Compatible con la interface de WhaleTracker (get_whale_pressure).
    Se actualiza en background cada UPDATE_INTERVAL segundos.
    """

    UPDATE_INTERVAL = 15  # segundos entre consultas on-chain (10 bloques * 2s = 20s de cobertura)

    def __init__(self, api_key: str = None, timeout: int = 15):
        self.api_key = api_key or os.getenv("ALCHEMY_API_KEY", "")
        if not self.api_key:
            raise ValueError("ALCHEMY_API_KEY no configurado")

        self.rpc_url = f"https://polygon-mainnet.g.alchemy.com/v2/{self.api_key}"
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

        # token_id -> {buy_volume, sell_volume, ratio, whale_count, updated_at}
        self._pressure: dict[str, dict] = {}
        self._lock = threading.Lock()

        # Ultimo bloque procesado
        self._last_block: int = 0

        # Thread de actualizacion en background
        self._running = False
        self._thread: threading.Thread | None = None

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    def start(self):
        """Inicia el thread de actualizacion en background."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="AlchemyWhale")
        self._thread.start()
        logger.info("AlchemyWhaleTracker: started (Polygon JSON-RPC)")

    def stop(self):
        self._running = False

    # ── Public API (compatible con WhaleTracker) ───────────────────────────────

    def get_whale_pressure(self, token_id: str) -> dict:
        """
        Retorna presion de ballenas para el token dado.
        Datos actualizados por el thread de background cada 30 segundos.

        Returns:
            {buy_volume, sell_volume, ratio, whale_count}
            donde volumes son en USDC (float, no wei)
        """
        empty = {"buy_volume": 0.0, "sell_volume": 0.0, "ratio": 0.5, "whale_count": 0}

        with self._lock:
            entry = self._pressure.get(token_id)

        if entry is None:
            return empty

        # Si los datos son muy viejos, devolver neutral en vez de stale
        age = time.time() - entry.get("updated_at", 0)
        if age > PRESSURE_CACHE_TTL * 5:
            return empty

        return {
            "buy_volume":  entry["buy_volume"],
            "sell_volume": entry["sell_volume"],
            "ratio":       entry["ratio"],
            "whale_count": entry["whale_count"],
        }

    # ── Background loop ────────────────────────────────────────────────────────

    def _run_loop(self):
        # Esperar un poco antes del primer fetch para no saturar al inicio
        time.sleep(5)
        while self._running:
            try:
                self._fetch_and_update()
            except Exception as e:
                logger.warning(f"AlchemyWhale: update error: {e}")
            time.sleep(self.UPDATE_INTERVAL)

    def _fetch_and_update(self):
        """Obtiene eventos recientes y actualiza la tabla de presion."""
        latest_block = self._get_latest_block()
        if not latest_block:
            return

        from_block = max(self._last_block + 1, latest_block - LOOKBACK_BLOCKS)
        if from_block > latest_block:
            return

        logs = self._get_order_filled_logs(from_block, latest_block)
        if logs is None:
            return

        # Acumular nuevas presiones (incrementales sobre datos existentes)
        new_pressure: dict[str, dict] = defaultdict(lambda: {
            "buy_volume": 0.0, "sell_volume": 0.0, "whale_count": 0
        })

        for log in logs:
            try:
                token_id, side, usdc_amount = self._decode_log(log)
                if token_id is None:
                    continue

                usdc_usd = usdc_amount / 10**6  # convertir a dolares

                if usdc_amount < WHALE_USDC_THRESHOLD:
                    continue  # ignorar trades pequenos

                if side == "BUY":
                    new_pressure[token_id]["buy_volume"] += usdc_usd
                else:
                    new_pressure[token_id]["sell_volume"] += usdc_usd
                new_pressure[token_id]["whale_count"] += 1

            except Exception as e:
                logger.debug(f"AlchemyWhale: log decode error: {e}")
                continue

        now = time.time()
        with self._lock:
            # Mezclar con datos existentes (rolling window)
            for token_id, data in new_pressure.items():
                existing = self._pressure.get(token_id, {
                    "buy_volume": 0.0, "sell_volume": 0.0,
                    "ratio": 0.5, "whale_count": 0, "updated_at": 0
                })

                # Decay exponencial del historial viejo (50% por cada update)
                decay = 0.7
                buy  = existing["buy_volume"]  * decay + data["buy_volume"]
                sell = existing["sell_volume"] * decay + data["sell_volume"]
                wc   = int(existing["whale_count"] * decay) + data["whale_count"]

                total = buy + sell
                ratio = (buy / total) if total > 0 else 0.5

                self._pressure[token_id] = {
                    "buy_volume":  round(buy, 2),
                    "sell_volume": round(sell, 2),
                    "ratio":       round(ratio, 4),
                    "whale_count": wc,
                    "updated_at":  now,
                }

            self._last_block = latest_block

        if new_pressure:
            logger.info(
                f"AlchemyWhale: blocks {from_block}-{latest_block} | "
                f"{len(logs)} logs | {len(new_pressure)} tokens actualizados"
            )
        else:
            logger.debug(
                f"AlchemyWhale: blocks {from_block}-{latest_block} | "
                f"{len(logs)} logs | sin actividad whale"
            )

    # ── JSON-RPC helpers ───────────────────────────────────────────────────────

    def _rpc(self, method: str, params: list) -> dict:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }
        resp = self.session.post(self.rpc_url, json=payload, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"RPC error: {data['error']}")
        return data.get("result")

    def _get_latest_block(self) -> int | None:
        try:
            result = self._rpc("eth_blockNumber", [])
            return int(result, 16)
        except Exception as e:
            logger.warning(f"AlchemyWhale: eth_blockNumber failed: {e}")
            return None

    def _get_order_filled_logs(self, from_block: int, to_block: int) -> list | None:
        all_logs = []
        for exchange in [CTF_EXCHANGE, NEG_RISK_EXCHANGE]:
            try:
                params = [{
                    "fromBlock": hex(from_block),
                    "toBlock":   hex(to_block),   # siempre numero fijo, nunca "latest"
                    "address":   exchange,
                    "topics":    [ORDER_FILLED_TOPIC],
                }]
                logs = self._rpc("eth_getLogs", params)
                if isinstance(logs, list):
                    all_logs.extend(logs)
            except Exception as e:
                logger.warning(f"AlchemyWhale: eth_getLogs failed: {e}")
        return all_logs

    # ── ABI decoding ───────────────────────────────────────────────────────────

    def _decode_log(self, log: dict) -> tuple[str | None, str | None, int]:
        """
        Decodifica un evento OrderFilled.

        topics: [eventSig, orderHash, makerAddress, takerAddress]
        data:   [makerAssetId, takerAssetId, makerAmountFilled, takerAmountFilled, fee]
                cada uno es 32 bytes (64 hex chars)

        Retorna: (token_id_hex, "BUY"|"SELL", usdc_amount_wei)
        """
        data_hex = log.get("data", "0x")
        if data_hex.startswith("0x"):
            data_hex = data_hex[2:]

        # Necesitamos 5 * 32 bytes = 320 bytes = 640 hex nybbles (2 per byte)
        # 5 uint256 × 32 bytes × 2 hex chars = 320 hex chars
        if len(data_hex) < 320:
            return None, None, 0

        chunks = [data_hex[i:i+64] for i in range(0, 320, 64)]
        maker_asset_id      = int(chunks[0], 16)
        taker_asset_id      = int(chunks[1], 16)
        maker_amount_filled = int(chunks[2], 16)
        taker_amount_filled = int(chunks[3], 16)
        # fee = int(chunks[4], 16)  # no usado

        # Determinar cual es el token de prediccion y cual es USDC
        # USDC tiene un assetId que es su address Polygon (numero < 2^161)
        maker_is_usdc = maker_asset_id < USDC_ASSET_THRESHOLD
        taker_is_usdc = taker_asset_id < USDC_ASSET_THRESHOLD

        if maker_is_usdc and not taker_is_usdc:
            # Maker paga USDC, recibe token → es una compra de token (BUY)
            token_id  = hex(taker_asset_id)
            usdc_amt  = maker_amount_filled
            side      = "BUY"
        elif taker_is_usdc and not maker_is_usdc:
            # Taker paga USDC, maker recibe USDC → maker esta vendiendo token (SELL)
            token_id  = hex(maker_asset_id)
            usdc_amt  = taker_amount_filled
            side      = "SELL"
        else:
            # token-to-token o USDC-to-USDC: no aplica
            return None, None, 0

        # Los token_ids de Polymarket son strings decimales en la Gamma API
        # Convertir hex -> decimal string para compatibilidad
        token_id_dec = str(int(token_id, 16))
        return token_id_dec, side, usdc_amt
