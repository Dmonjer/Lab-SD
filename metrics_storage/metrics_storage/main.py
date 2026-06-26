"""
main.py - Almacenamiento de Métricas (Metrics Storage)
Recibe eventos del sistema (hits, misses, latencias) y los persiste en CSV.
También expone endpoints para consultar estadísticas agregadas en tiempo real.
"""
import os
import csv
import time
import threading
from pathlib import Path
from collections import defaultdict
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional
import json
from kafka import KafkaProducer

# -------------------------------------------------------------------------
# Configuración
# -------------------------------------------------------------------------
METRICS_DIR = Path(os.getenv("METRICS_DIR", "/metrics"))
METRICS_FILE = METRICS_DIR / "events.csv"
EXPERIMENTS_FILE = METRICS_DIR / "experiments.csv"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
kafka_producer = None

app = FastAPI(title="Metrics Storage", version="1.0")

# Lock para escritura thread-safe al CSV
write_lock = threading.Lock()

# Contador en memoria para estadísticas rápidas
stats = defaultdict(lambda: {
    "hits": 0, "misses": 0, "latencies": [], "evictions": 0,
    "retries": 0, "recoveries": 0, "dlq": 0, "consumer_lag": 0, "recovery_time": 0.0
})
experiments = []

# -------------------------------------------------------------------------
# Inicialización de archivos CSV
# -------------------------------------------------------------------------
EVENTS_HEADERS = ["timestamp", "event_type", "query_type", "zone_id", "cache_key", "latency_ms"]
EXPERIMENTS_HEADERS = [
    "experiment_id", "start_time", "end_time", "distribution", "zipf_alpha",
    "n_requests", "request_rate", "sent", "success", "errors", "hits", "misses"
]


@app.on_event("startup")
def startup():
    global kafka_producer
    for i in range(10):
        try:
            kafka_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Kafka Producer inicializado.")
            break
        except Exception as e:
            print(f"Error inicializando Kafka Producer (intento {i+1}/10): {e}")
            time.sleep(3)

    METRICS_DIR.mkdir(parents=True, exist_ok=True)

    if not METRICS_FILE.exists():
        with open(METRICS_FILE, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=EVENTS_HEADERS)
            writer.writeheader()

    if not EXPERIMENTS_FILE.exists():
        with open(EXPERIMENTS_FILE, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=EXPERIMENTS_HEADERS)
            writer.writeheader()

    print(f"Métricas se guardarán en: {METRICS_DIR}")


# -------------------------------------------------------------------------
# Modelos
# -------------------------------------------------------------------------
class MetricEvent(BaseModel):
    event_type: str           # "hit", "miss", "retry", "recovery", "dlq", "consumer_lag", "recovery_time"
    query_type: str           # Q1..Q5
    zone_id: str              # Z1..Z5
    cache_key: str
    latency_ms: float
    timestamp: Optional[float] = None


class ExperimentStart(BaseModel):
    n_requests: int
    distribution: str
    zipf_alpha: Optional[float] = 1.5
    request_rate: Optional[float] = 10.0
    timestamp: Optional[float] = None


class ExperimentEnd(BaseModel):
    timestamp: Optional[float] = None
    sent: int = 0
    success: int = 0
    errors: int = 0
    hits: int = 0
    misses: int = 0


current_experiment_id = 0
current_experiment_meta = {}


# -------------------------------------------------------------------------
# Endpoints de Métricas
# -------------------------------------------------------------------------
@app.get("/health")
def health():
    return {
        "status": "ok",
        "events_file": str(METRICS_FILE),
        "total_events_in_memory": sum(s["hits"] + s["misses"] for s in stats.values()),
    }


@app.post("/event")
def receive_event(event: MetricEvent):
    ts = event.timestamp or time.time()

    # Escribir al CSV
    row = {
        "timestamp": ts,
        "event_type": event.event_type,
        "query_type": event.query_type,
        "zone_id": event.zone_id,
        "cache_key": event.cache_key,
        "latency_ms": event.latency_ms,
    }
    with write_lock:
        with open(METRICS_FILE, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=EVENTS_HEADERS)
            writer.writerow(row)

    # Actualizar stats en memoria
    key = f"{event.query_type}:{event.zone_id}"
    if event.event_type == "hit":
        stats[key]["hits"] += 1
    elif event.event_type == "miss":
        stats[key]["misses"] += 1
    elif event.event_type == "retry":
        stats[key]["retries"] += 1
    elif event.event_type == "recovery":
        stats[key]["recoveries"] += 1
    elif event.event_type == "dlq":
        stats[key]["dlq"] += 1
    elif event.event_type == "consumer_lag":
        stats[key]["consumer_lag"] = event.latency_ms
    elif event.event_type == "recovery_time":
        stats[key]["recovery_time"] = event.latency_ms

    if event.event_type in ["hit", "miss", "recovery"]:
        stats[key]["latencies"].append(event.latency_ms)

    if kafka_producer:
        try:
            kafka_producer.send("metrics-topic", row)
        except Exception as e:
            print(f"Error enviando metrica a kafka: {e}")

    return {"status": "ok"}


@app.post("/experiment/start")
def experiment_start(config: ExperimentStart):
    global current_experiment_id, current_experiment_meta
    current_experiment_id += 1
    current_experiment_meta = {
        "experiment_id": current_experiment_id,
        "start_time": config.timestamp or time.time(),
        "distribution": config.distribution,
        "zipf_alpha": config.zipf_alpha,
        "n_requests": config.n_requests,
        "request_rate": config.request_rate,
    }
    print(f"Experimento #{current_experiment_id} iniciado: {config.distribution}, {config.n_requests} requests")
    return {"experiment_id": current_experiment_id}


@app.post("/experiment/end")
def experiment_end(result: ExperimentEnd):
    meta = current_experiment_meta.copy()
    meta.update({
        "end_time": result.timestamp or time.time(),
        "sent": result.sent,
        "success": result.success,
        "errors": result.errors,
        "hits": result.hits,
        "misses": result.misses,
    })

    with write_lock:
        with open(EXPERIMENTS_FILE, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=EXPERIMENTS_HEADERS)
            writer.writerow(meta)

    experiments.append(meta)
    hit_rate = result.hits / result.sent * 100 if result.sent > 0 else 0
    print(f"Experimento #{meta.get('experiment_id')} terminado. Hit rate: {hit_rate:.1f}%")
    return {"status": "ok", **meta}


@app.get("/stats")
def get_stats():
    """Estadísticas agregadas en tiempo real."""
    total_hits = sum(s["hits"] for s in stats.values())
    total_misses = sum(s["misses"] for s in stats.values())
    total_retries = sum(s["retries"] for s in stats.values())
    total_recoveries = sum(s["recoveries"] for s in stats.values())
    total_dlq = sum(s["dlq"] for s in stats.values())
    total = total_hits + total_misses

    all_latencies = [l for s in stats.values() for l in s["latencies"]]
    all_latencies_sorted = sorted(all_latencies)

    def percentile(data, p):
        if not data:
            return 0
        idx = max(0, int(len(data) * p / 100) - 1)
        return round(data[idx], 3)

    return {
        "total_requests": total,
        "total_hits": total_hits,
        "total_misses": total_misses,
        "hit_rate": round(total_hits / total * 100, 2) if total > 0 else 0,
        "retry_rate": round(total_retries / total_misses * 100, 2) if total_misses > 0 else 0,
        "recovery_rate": round(total_recoveries / total_retries * 100, 2) if total_retries > 0 else 0,
        "dlq_rate": round(total_dlq / total_misses * 100, 2) if total_misses > 0 else 0,
        "backlog_size": sum(s["consumer_lag"] for s in stats.values()),
        "last_recovery_time_ms": max([s["recovery_time"] for s in stats.values()] + [0]),
        "latency_p50_ms": percentile(all_latencies_sorted, 50),
        "latency_p95_ms": percentile(all_latencies_sorted, 95),
        "avg_latency_ms": round(sum(all_latencies) / len(all_latencies), 3) if all_latencies else 0,
        "by_query_type": {
            k: {
                "hits": v["hits"],
                "misses": v["misses"],
                "retries": v["retries"],
                "dlq": v["dlq"],
                "hit_rate": round(v["hits"] / (v["hits"] + v["misses"]) * 100, 2) if (v["hits"] + v["misses"]) > 0 else 0,
            }
            for k, v in stats.items()
        },
    }


@app.get("/experiments")
def get_experiments():
    return experiments


@app.delete("/reset")
def reset_stats():
    """Resetea las estadísticas en memoria (no borra los CSV)."""
    stats.clear()
    return {"status": "ok", "message": "Estadísticas en memoria reseteadas."}
