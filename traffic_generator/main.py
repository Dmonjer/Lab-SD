"""
main.py - Generador de Tráfico (Traffic Generator)
Simula solicitudes publicando consultas directamente en el tópico de Kafka consultas_geoespaciales.
"""
import os
import time
import random
import asyncio
import uuid
import json
import httpx
import numpy as np
from confluent_kafka import Producer
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
from typing import Optional

# -------------------------------------------------------------------------
# Configuración
# -------------------------------------------------------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
METRICS_URL = os.getenv("METRICS_URL", "http://localhost:8003")
DISTRIBUTION = os.getenv("DISTRIBUTION", "zipf")
N_REQUESTS = int(os.getenv("N_REQUESTS", "1000"))
ZIPF_ALPHA = float(os.getenv("ZIPF_ALPHA", "1.5"))
REQUEST_RATE = float(os.getenv("REQUEST_RATE", "10"))  # req/sec

app = FastAPI(title="Traffic Generator", version="1.0")

# Zonas y tipos de query según la especificación
ZONES = ["Z1", "Z2", "Z3", "Z4", "Z5"]
QUERY_TYPES = ["Q1", "Q2", "Q3", "Q4", "Q5"]
CONFIDENCE_LEVELS = [0.0, 0.5, 0.7]

# Popularidades de zonas (Zipf favorece Z1 >> Z2 >> ... >> Z5)
ZONE_WEIGHTS_UNIFORM = [0.2, 0.2, 0.2, 0.2, 0.2]
QUERY_WEIGHTS = [0.40, 0.20, 0.20, 0.10, 0.10]

is_running = False
kafka_producer: Producer = None


@app.on_event("startup")
def startup():
    global kafka_producer
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'client.id': 'traffic_generator_producer'
    }
    kafka_producer = Producer(conf)
    print(f"Conectado a Kafka como productor en {KAFKA_BROKER}")


@app.on_event("shutdown")
def shutdown():
    if kafka_producer:
        kafka_producer.flush()


def get_zipf_weights(n: int, alpha: float) -> list[float]:
    ranks = np.arange(1, n + 1)
    weights = 1.0 / np.power(ranks, alpha)
    return (weights / weights.sum()).tolist()


def generate_query(distribution: str, alpha: float) -> dict:
    if distribution == "zipf":
        zone_weights = get_zipf_weights(len(ZONES), alpha)
    else:
        zone_weights = ZONE_WEIGHTS_UNIFORM

    zone_id = random.choices(ZONES, weights=zone_weights, k=1)[0]
    query_type = random.choices(QUERY_TYPES, weights=QUERY_WEIGHTS, k=1)[0]
    confidence_min = random.choice(CONFIDENCE_LEVELS)
    bins = random.choice([5, 10])

    query = {
        "zone_id": zone_id,
        "query_type": query_type,
        "confidence_min": confidence_min,
        "bins": bins,
    }

    if query_type == "Q4":
        remaining = [z for z in ZONES if z != zone_id]
        query["zone_id_b"] = random.choice(remaining)

    return query


# -------------------------------------------------------------------------
# Modelos
# -------------------------------------------------------------------------
class RunConfig(BaseModel):
    n_requests: Optional[int] = None
    distribution: Optional[str] = None
    zipf_alpha: Optional[float] = None
    request_rate: Optional[float] = None


class StatusResponse(BaseModel):
    is_running: bool
    message: str


# -------------------------------------------------------------------------
# Lógica de ejecución del tráfico
# -------------------------------------------------------------------------
async def run_traffic(n: int, dist: str, alpha: float, rate: float):
    global is_running
    is_running = True
    print(f"Iniciando tráfico: {n} requests, distribución={dist}, alpha={alpha}, rate={rate} req/s")

    delay = 1.0 / rate
    results = {"sent": 0, "success": 0, "errors": 0, "hits": 0, "misses": 0}

    async with httpx.AsyncClient(timeout=15.0) as client:
        # Notificar inicio al servicio de métricas
        try:
            await client.post(f"{METRICS_URL}/experiment/start", json={
                "n_requests": n,
                "distribution": dist,
                "zipf_alpha": alpha,
                "request_rate": rate,
                "timestamp": time.time(),
            })
        except Exception:
            pass

        for i in range(n):
            query = generate_query(dist, alpha)
            t_send = time.time()
            try:
                payload = {
                    "id": f"req_{i}_{uuid.uuid4().hex[:8]}",
                    "req": query,
                    "retries": 0,
                    "t_start": t_send
                }
                kafka_producer.produce('consultas_geoespaciales', value=json.dumps(payload).encode('utf-8'))
                kafka_producer.poll(0)
                results["success"] += 1
            except Exception as e:
                results["errors"] += 1
                print(f"  Error en request {i}: {e}")

            results["sent"] += 1

            # Esperar para mantener el rate
            elapsed = time.time() - t_send
            wait = max(0.0, delay - elapsed)
            if wait > 0:
                await asyncio.sleep(wait)

            if (i + 1) % 100 == 0:
                print(f"  Progreso: {i+1}/{n} enviados a Kafka.")

    # Esperar un par de segundos a que se vacíe la cola local del productor
    kafka_producer.flush()

    # Notificar fin al servicio de métricas
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            await client.post(f"{METRICS_URL}/experiment/end", json={
                "timestamp": time.time(),
                **results,
            })
        except Exception:
            pass

    print(f"Tráfico completado: {results}")
    is_running = False


# -------------------------------------------------------------------------
# Endpoints
# -------------------------------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok", "is_running": is_running}


@app.post("/run", response_model=StatusResponse)
async def start_traffic(config: RunConfig, background_tasks: BackgroundTasks):
    global is_running
    if is_running:
        return StatusResponse(is_running=True, message="El generador de tráfico ya está en ejecución.")

    n = config.n_requests or N_REQUESTS
    dist = config.distribution or DISTRIBUTION
    alpha = config.zipf_alpha or ZIPF_ALPHA
    rate = config.request_rate or REQUEST_RATE

    background_tasks.add_task(run_traffic, n, dist, alpha, rate)
    return StatusResponse(is_running=True, message=f"Iniciando {n} requests con distribución '{dist}'.")


@app.post("/stop")
def stop_traffic():
    global is_running
    is_running = False
    return {"message": "Señal de parada enviada."}


@app.get("/status")
def status():
    return {"is_running": is_running}
