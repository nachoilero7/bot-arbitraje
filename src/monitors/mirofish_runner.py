"""
MiroFishRunner — Integración completa con el servidor MiroFish real.

Requiere que MiroFish esté corriendo localmente en localhost:5001.
Setup: git clone https://github.com/666ghj/MiroFish && npm run setup:all && npm run dev

Pipeline completo por mercado:
  1. POST /api/graph/ontology/generate  — genera ontología de la pregunta
  2. POST /api/graph/build              — construye grafo Zep (async, ~5min)
  3. POST /api/simulation/create        — crea simulación
  4. POST /api/simulation/prepare       — genera perfiles de agentes (async, ~5min)
  5. POST /api/simulation/start         — corre OASIS (async, ~15min)
  6. POST /api/report/generate          — genera reporte (async, ~5min)
  7. POST /api/report/chat              — extrae probabilidad del reporte

Costo estimado: ~$0.01 por mercado (GPT-4o-mini + Gemini 2.0 Flash free)
Tiempo por mercado: 20-45 minutos
"""
import json
import os
import threading
import time
from datetime import datetime, timezone

import requests

from src.utils.logger import get_logger

logger = get_logger(__name__)

MIROFISH_BASE   = os.getenv("MIROFISH_URL", "http://localhost:5001")
CACHE_PATH      = "data/mirofish_cache.json"

# Intervalos configurables
CYCLE_INTERVAL  = int(os.getenv("MIROFISH_INTERVAL_SECS", "3600"))   # 1h entre ciclos
MAX_MARKETS     = int(os.getenv("MIROFISH_MAX_MARKETS", "2"))         # 2 mercados/ciclo
MIN_LLM_EDGE    = float(os.getenv("MIROFISH_MIN_EDGE", "0.08"))
MIN_LIQ_USD     = float(os.getenv("MIROFISH_MIN_LIQ", "1000"))
MIN_DAYS        = int(os.getenv("MIROFISH_MIN_DAYS", "2"))
MAX_DAYS        = int(os.getenv("MIROFISH_MAX_DAYS", "60"))

# Timeouts de polling (segundos)
TIMEOUT_GRAPH    = int(os.getenv("MIROFISH_TIMEOUT_GRAPH", "600"))    # 10 min
TIMEOUT_PREPARE  = int(os.getenv("MIROFISH_TIMEOUT_PREPARE", "600"))  # 10 min
TIMEOUT_SIM      = int(os.getenv("MIROFISH_TIMEOUT_SIM", "2400"))     # 40 min
TIMEOUT_REPORT   = int(os.getenv("MIROFISH_TIMEOUT_REPORT", "900"))   # 15 min
POLL_INTERVAL    = int(os.getenv("MIROFISH_POLL_INTERVAL", "15"))     # 15s entre polls

GAMMA_API = "https://gamma-api.polymarket.com/markets"

# Categorías aptas para simulación social (donde hay opinión pública y debate)
LLM_CATEGORIES = {
    "politics", "political", "election", "government",
    "economics", "economy", "finance", "fed", "interest rate",
    "geopolitics", "international", "war", "conflict", "diplomacy",
    "sports", "football", "basketball", "soccer", "tennis",
    "science", "climate", "technology", "ai",
    "entertainment", "awards", "social",
}

CRYPTO_KEYWORDS = {
    "btc", "bitcoin", "eth", "ethereum", "crypto", "solana", "doge",
    "will be above", "will be below", "price", "reach", "higher than",
    "lower than", "$", "usd at",
}

PROBABILITY_QUESTION = (
    "Based on this multi-agent simulation, what is the probability (0.0 to 1.0) "
    "that the following event resolves YES: \"{question}\" "
    "Analyze the agent opinions, social dynamics, and knowledge graph evidence. "
    "Respond ONLY with valid JSON: "
    '{{\"probability\": 0.XX, \"confidence\": \"low|medium|high\", \"key_insight\": \"one sentence max 120 chars\"}}'
)


class MiroFishRunner:
    """
    Background worker que orquesta el pipeline completo de MiroFish.
    Requiere MiroFish server en MIROFISH_URL (default: localhost:5001).
    """

    def __init__(self, proxy: str = None):
        self._proxy    = proxy
        self._running  = False
        self._thread: threading.Thread | None = None
        self._cache: dict = self._load_cache()
        self._session  = requests.Session()
        if proxy:
            self._session.proxies = {"http": proxy, "https": proxy}

    # ── Public API ──────────────────────────────────────────────────────────────

    def start(self):
        if not self._check_server():
            logger.warning(
                f"[MIROFISH] Server no disponible en {MIROFISH_BASE}. "
                "Inicia MiroFish: cd MiroFish && npm run dev"
            )
            return
        self._running = True
        self._thread  = threading.Thread(target=self._loop, daemon=True, name="mirofish-runner")
        self._thread.start()
        logger.info(f"[MIROFISH] Runner iniciado. URL={MIROFISH_BASE} ciclo={CYCLE_INTERVAL}s max={MAX_MARKETS}/ciclo")

    def stop(self):
        self._running = False

    def is_running(self) -> bool:
        return self._running and (self._thread is not None) and self._thread.is_alive()

    # ── Loop ────────────────────────────────────────────────────────────────────

    def _loop(self):
        self._run_cycle()
        while self._running:
            for _ in range(CYCLE_INTERVAL):
                if not self._running:
                    return
                time.sleep(1)
            self._run_cycle()

    def _run_cycle(self):
        try:
            candidates = self._fetch_candidates()
            if not candidates:
                logger.info("[MIROFISH] Sin candidatos aptos en este ciclo")
                return
            logger.info(f"[MIROFISH] Ciclo iniciado: {len(candidates)} candidatos, procesando top {MAX_MARKETS}")
            for market in candidates[:MAX_MARKETS]:
                if not self._running:
                    break
                self._process_market(market)
        except Exception as e:
            logger.error(f"[MIROFISH] Error en ciclo principal: {e}", exc_info=True)

    # ── Pipeline completo ───────────────────────────────────────────────────────

    def _process_market(self, market: dict):
        cid      = market["conditionId"]
        question = market["question"]
        logger.info(f"[MIROFISH] Iniciando pipeline: {question[:70]}")

        try:
            # Paso 1-2: Construir grafo de conocimiento
            project_id = self._build_graph(market)
            if not project_id:
                return

            # Paso 3-5: Ejecutar simulación OASIS
            sim_id = self._run_simulation(project_id, market)
            if not sim_id:
                self._cleanup_project(project_id)
                return

            # Paso 6: Generar reporte
            if not self._generate_report(sim_id):
                self._cleanup_project(project_id)
                return

            # Paso 7: Extraer probabilidad via chat con ReportAgent
            result = self._extract_probability(sim_id, question)
            if result:
                probability  = result["probability"]
                confidence   = result["confidence"]
                key_insight  = result["key_insight"]
                market_price = market["market_price"]
                edge         = abs(probability - market_price) - 0.02

                self._cache[cid] = {
                    "question":        question,
                    "category":        market["category"],
                    "market_price":    market_price,
                    "llm_probability": probability,
                    "confidence":      confidence,
                    "edge":            round(edge, 5),
                    "key_factor":      key_insight,
                    "token_id":        market.get("token_ids", [""])[0],
                    "liquidity_usd":   market["liquidity_usd"],
                    "analyzed_at":     datetime.utcnow().isoformat(),
                    "sim_id":          sim_id,
                    "project_id":      project_id,
                }
                self._save_cache()

                if edge >= MIN_LLM_EDGE:
                    logger.info(
                        f"[MIROFISH] *** EDGE DETECTADO *** | {question[:60]} | "
                        f"mkt={market_price:.2f} llm={probability:.2f} edge={edge:.3f} [{confidence}] | {key_insight[:60]}"
                    )
                else:
                    logger.info(
                        f"[MIROFISH] Sin edge suficiente | {question[:55]} | "
                        f"mkt={market_price:.2f} llm={probability:.2f} edge={edge:.3f}"
                    )

        except Exception as e:
            logger.error(f"[MIROFISH] Error procesando {question[:50]}: {e}", exc_info=True)

    # ── Paso 1-2: Graph ─────────────────────────────────────────────────────────

    def _build_graph(self, market: dict) -> str | None:
        question     = market["question"]
        category     = market["category"]
        end_date     = market["end_date"]
        market_price = market["market_price"]

        sim_req = (
            f"Predict the outcome of this binary prediction market question: '{question}'. "
            f"Category: {category}. Market closes: {end_date}. "
            f"Current market consensus probability: {market_price:.1%}. "
            f"Simulate public opinion, expert debate, and social dynamics to estimate "
            f"the true probability of this event resolving YES."
        )

        # MiroFish requiere al menos un archivo de contexto para construir el grafo.
        # Generamos un documento markdown con toda la información relevante del mercado.
        context_md = f"""# Prediction Market: {question}

## Market Details
- **Question**: {question}
- **Category**: {category}
- **Resolution Date**: {end_date}
- **Current Market Price (YES probability)**: {market_price:.1%}

## Analysis Objective
Simulate public opinion, expert debate, and social dynamics to predict the true probability
of this binary prediction market resolving YES.

## Key Questions to Investigate
1. What factors strongly support the YES outcome?
2. What factors strongly support the NO outcome?
3. How does current public sentiment lean on this topic?
4. What are the key expert opinions and data sources relevant to this event?
5. What historical base rates apply to similar events in the {category} category?
6. What could cause the market consensus of {market_price:.0%} to be significantly wrong?

## Context
This is a binary prediction market. The current crowd consensus places the probability at
{market_price:.1%}. The simulation should explore whether this price reflects the true
probability or if there is a systematic bias in the crowd's assessment.
""".encode("utf-8")

        logger.info(f"[MIROFISH] [1/6] Generando ontología con documento de contexto...")
        try:
            resp = self._session.post(
                f"{MIROFISH_BASE}/api/graph/ontology/generate",
                files=[("files", ("market_context.md", context_md, "text/markdown"))],
                data={
                    "simulation_requirement": sim_req,
                    "project_name": f"polyedge_{market['conditionId'][:12]}",
                },
                timeout=120,
            )
            resp.raise_for_status()
            result = resp.json()
            project_id = result.get("data", {}).get("project_id")
            if not project_id:
                logger.error(f"[MIROFISH] ontology/generate no devolvió project_id: {result}")
                return None
            logger.info(f"[MIROFISH] Ontología generada. project_id={project_id}")
        except Exception as e:
            # Capturar cuerpo del error para diagnóstico
            body = ""
            try:
                body = e.response.json() if hasattr(e, "response") and e.response is not None else ""
            except Exception:
                pass
            logger.error(f"[MIROFISH] ontology/generate error: {e} | body={body}")
            return None

        logger.info(f"[MIROFISH] [2/6] Construyendo grafo Zep (puede tardar ~5min)...")
        try:
            resp = self._post("/api/graph/build", json={
                "project_id": project_id,
                "graph_name": f"polyedge_{market['conditionId'][:12]}",
            })
            task_id = resp.get("data", {}).get("task_id")
            if not task_id:
                logger.error(f"[MIROFISH] graph/build sin task_id: {resp}")
                return None

            if not self._poll_task(f"/api/graph/task/{task_id}", TIMEOUT_GRAPH, "graph build"):
                return None
        except Exception as e:
            logger.error(f"[MIROFISH] graph/build error: {e}")
            return None

        return project_id

    # ── Paso 3-5: Simulation ────────────────────────────────────────────────────

    def _run_simulation(self, project_id: str, market: dict) -> str | None:
        logger.info(f"[MIROFISH] [3/6] Creando simulación...")
        try:
            resp = self._post("/api/simulation/create", json={
                "project_id": project_id,
                "enable_twitter": True,
                "enable_reddit":  True,
            })
            sim_id = resp.get("data", {}).get("simulation_id")
            if not sim_id:
                logger.error(f"[MIROFISH] simulation/create sin sim_id: {resp}")
                return None
            logger.info(f"[MIROFISH] Simulación creada. sim_id={sim_id}")
        except Exception as e:
            logger.error(f"[MIROFISH] simulation/create error: {e}")
            return None

        logger.info(f"[MIROFISH] [4/6] Preparando perfiles de agentes (~5min)...")
        try:
            resp = self._post("/api/simulation/prepare", json={
                "simulation_id": sim_id,
                "use_llm_for_profiles": True,
                "parallel_profile_count": 5,
            })
            task_id = resp.get("data", {}).get("task_id")
            if task_id:
                if not self._poll_task_body("/api/simulation/prepare/status", task_id, TIMEOUT_PREPARE, "sim prepare"):
                    return None
            # Si no hay task_id, la respuesta ya es completada (raro pero posible)
        except Exception as e:
            logger.error(f"[MIROFISH] simulation/prepare error: {e}")
            return None

        logger.info(f"[MIROFISH] [5/6] Corriendo simulación OASIS (~15-30min)...")
        try:
            self._post("/api/simulation/start", json={
                "simulation_id": sim_id,
                "platform":      "parallel",
                "max_rounds":    int(os.getenv("OASIS_DEFAULT_MAX_ROUNDS", "10")),
                "enable_graph_memory_update": False,
            })
            if not self._poll_sim_status(sim_id):
                return None
        except Exception as e:
            logger.error(f"[MIROFISH] simulation/start error: {e}")
            return None

        return sim_id

    def _poll_sim_status(self, sim_id: str) -> bool:
        """Espera a que la simulación llegue a 'completed' o 'stopped'."""
        deadline = time.time() + TIMEOUT_SIM
        last_log = 0
        while time.time() < deadline:
            if not self._running:
                return False
            try:
                resp = self._get(f"/api/simulation/{sim_id}/run-status")
                data = resp.get("data", {})
                status  = data.get("runner_status", "")
                pct     = data.get("progress_percent", 0)
                current = data.get("current_round", 0)
                total   = data.get("total_rounds", 0)

                if time.time() - last_log > 60:
                    logger.info(f"[MIROFISH] Sim running: {current}/{total} rounds ({pct:.1f}%)")
                    last_log = time.time()

                if status in ("completed", "stopped"):
                    logger.info(f"[MIROFISH] Simulación completa: {current} rounds")
                    return True
                if status == "failed":
                    logger.error(f"[MIROFISH] Simulación falló: {data}")
                    return False
            except Exception as e:
                logger.debug(f"[MIROFISH] poll sim status error: {e}")
            time.sleep(POLL_INTERVAL)
        logger.error(f"[MIROFISH] Timeout esperando simulación ({TIMEOUT_SIM}s)")
        return False

    # ── Paso 6: Report ──────────────────────────────────────────────────────────

    def _generate_report(self, sim_id: str) -> bool:
        logger.info(f"[MIROFISH] [6/6] Generando reporte...")
        try:
            resp = self._post("/api/report/generate", json={"simulation_id": sim_id})
            task_id = resp.get("data", {}).get("task_id")
            if task_id:
                return self._poll_task_body("/api/report/generate/status", task_id, TIMEOUT_REPORT, "report gen")
            # Si no hay task_id, puede que ya esté listo
            return True
        except Exception as e:
            logger.error(f"[MIROFISH] report/generate error: {e}")
            return False

    # ── Paso 7: Probability extraction ─────────────────────────────────────────

    def _extract_probability(self, sim_id: str, question: str) -> dict | None:
        msg = PROBABILITY_QUESTION.format(question=question)
        try:
            resp = self._post("/api/report/chat", json={
                "simulation_id": sim_id,
                "message":       msg,
                "chat_history":  [],
            })
            reply = resp.get("data", {}).get("response", "")
            if not reply:
                logger.error(f"[MIROFISH] report/chat sin respuesta: {resp}")
                return None

            # Extraer JSON de la respuesta (puede venir con texto extra)
            start = reply.find("{")
            end   = reply.rfind("}") + 1
            if start == -1 or end == 0:
                logger.error(f"[MIROFISH] report/chat sin JSON: {reply[:150]}")
                return None

            parsed = json.loads(reply[start:end])
            prob   = float(parsed.get("probability", -1))
            if not (0.0 <= prob <= 1.0):
                logger.error(f"[MIROFISH] Probabilidad inválida: {prob}")
                return None

            logger.info(
                f"[MIROFISH] Probabilidad extraída: {prob:.3f} "
                f"[{parsed.get('confidence','?')}] — {parsed.get('key_insight','')[:80]}"
            )
            return {
                "probability": prob,
                "confidence":  parsed.get("confidence", "low"),
                "key_insight": parsed.get("key_insight", ""),
            }
        except Exception as e:
            logger.error(f"[MIROFISH] report/chat error: {e}")
            return None

    # ── Fetch candidates ────────────────────────────────────────────────────────

    def _fetch_candidates(self) -> list[dict]:
        try:
            proxies = {"https": self._proxy, "http": self._proxy} if self._proxy else None
            resp = requests.get(
                GAMMA_API,
                params={"active": True, "closed": False, "limit": 200, "order": "liquidityNum", "ascending": False},
                timeout=15,
                proxies=proxies,
            )
            resp.raise_for_status()
            raw = resp.json()
            markets = raw if isinstance(raw, list) else raw.get("data", [])
        except Exception as e:
            logger.warning(f"[MIROFISH] Error fetch markets: {e}")
            return []

        candidates = []
        now = datetime.now(timezone.utc)

        for m in markets:
            try:
                cid = m.get("conditionId", "")

                liq = float(m.get("liquidityNum") or 0)
                if liq < MIN_LIQ_USD:
                    continue

                end_date_str = m.get("endDate") or m.get("end_date_iso") or ""
                if not end_date_str:
                    continue
                end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                days_left = (end_dt - now).days
                if not (MIN_DAYS <= days_left <= MAX_DAYS):
                    continue

                outcome_prices = m.get("outcomePrices", "[]")
                if isinstance(outcome_prices, str):
                    outcome_prices = json.loads(outcome_prices)
                if len(outcome_prices) != 2:
                    continue
                p_yes = float(outcome_prices[0])
                if not (0.15 <= p_yes <= 0.85):
                    continue

                question_lower = (m.get("question") or "").lower()
                if any(kw in question_lower for kw in CRYPTO_KEYWORDS):
                    continue

                category = ""
                events = m.get("events")
                if events and isinstance(events, list):
                    category = (events[0].get("category") or events[0].get("title") or "").lower()
                if not any(cat in category for cat in LLM_CATEGORIES):
                    continue

                token_ids = []
                raw_ids = m.get("clobTokenIds", "[]")
                if isinstance(raw_ids, str):
                    token_ids = json.loads(raw_ids)

                candidates.append({
                    "conditionId":  cid,
                    "question":     m.get("question", ""),
                    "category":     category,
                    "market_price": p_yes,
                    "liquidity_usd": liq,
                    "end_date":     end_dt.strftime("%Y-%m-%d"),
                    "token_ids":    token_ids,
                })
            except Exception:
                continue

        # Priorizar mercados sin análisis reciente
        def age_score(c: dict) -> float:
            cached = self._cache.get(c["conditionId"])
            if not cached:
                return float("inf")
            analyzed_at = datetime.fromisoformat(cached.get("analyzed_at", "1970-01-01"))
            return (datetime.utcnow() - analyzed_at).total_seconds()

        candidates.sort(key=age_score, reverse=True)
        logger.debug(f"[MIROFISH] {len(candidates)} candidatos (de {len(markets)} mercados)")
        return candidates

    # ── HTTP helpers ────────────────────────────────────────────────────────────

    def _get(self, path: str) -> dict:
        r = self._session.get(f"{MIROFISH_BASE}{path}", timeout=30)
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, json: dict = None, data: dict = None) -> dict:
        r = self._session.post(f"{MIROFISH_BASE}{path}", json=json, data=data, timeout=60)
        r.raise_for_status()
        return r.json()

    def _poll_task(self, path: str, timeout: int, label: str) -> bool:
        """Polling para endpoints GET /api/X/task/<task_id>."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if not self._running:
                return False
            try:
                resp   = self._get(path)
                status = resp.get("data", {}).get("status", "")
                pct    = resp.get("data", {}).get("progress", 0)
                logger.debug(f"[MIROFISH] {label}: {status} {pct}%")
                if status == "completed":
                    logger.info(f"[MIROFISH] {label} completado")
                    return True
                if status == "failed":
                    logger.error(f"[MIROFISH] {label} falló: {resp}")
                    return False
            except Exception as e:
                logger.debug(f"[MIROFISH] poll {label} error: {e}")
            time.sleep(POLL_INTERVAL)
        logger.error(f"[MIROFISH] Timeout en {label} ({timeout}s)")
        return False

    def _poll_task_body(self, path: str, task_id: str, timeout: int, label: str) -> bool:
        """Polling para endpoints POST /api/X/status con {task_id} en el body."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if not self._running:
                return False
            try:
                resp   = self._post(path, json={"task_id": task_id})
                status = resp.get("data", {}).get("status", "")
                pct    = resp.get("data", {}).get("progress", 0)
                logger.debug(f"[MIROFISH] {label}: {status} {pct}%")
                if status == "completed":
                    logger.info(f"[MIROFISH] {label} completado")
                    return True
                if status == "failed":
                    logger.error(f"[MIROFISH] {label} falló: {resp}")
                    return False
            except Exception as e:
                logger.debug(f"[MIROFISH] poll {label} error: {e}")
            time.sleep(POLL_INTERVAL)
        logger.error(f"[MIROFISH] Timeout en {label} ({timeout}s)")
        return False

    def _cleanup_project(self, project_id: str):
        """Limpia proyectos en MiroFish para no acumular basura."""
        try:
            self._session.delete(f"{MIROFISH_BASE}/api/graph/project/{project_id}", timeout=10)
        except Exception:
            pass

    def _check_server(self) -> bool:
        try:
            r = self._session.get(f"{MIROFISH_BASE}/api/graph/project/list", timeout=5)
            return r.status_code < 500
        except Exception:
            return False

    # ── Cache ────────────────────────────────────────────────────────────────────

    def _load_cache(self) -> dict:
        if os.path.exists(CACHE_PATH):
            try:
                with open(CACHE_PATH, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

    def _save_cache(self):
        os.makedirs("data", exist_ok=True)
        with open(CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(self._cache, f, indent=2, ensure_ascii=False)
