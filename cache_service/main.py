"""
main.py - Sistema de Caché (Cache Service)
Corre un servidor FastAPI para estadísticas e inicia un hilo de fondo (Consumer)
que lee consultas de Kafka, interactúa con Redis y delega misses al Response Generator.
"""
import os
import json
import time
import hashlib
import threading
import httpx
import redis
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Optional

# -------------------------------------------------------------------------
# Configuración
# -------------------------------------------------------------------------
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
METRICS_URL = os.getenv("METRICS_URL", "http://localhost:8003")
RESPONSE_GEN_URL = os.getenv("RESPONSE_GEN_URL", "http://response_generator:8001")
CACHE_TTL = int(os.getenv("CACHE_TTL", "300"))

app = FastAPI(title="Cache Service & Kafka Consumer", version="1.0")

# Clientes globales
redis_client: redis.Redis = None
http_client: httpx.Client = None  # Síncrono para el hilo de fondo
kafka_producer: Producer = None

@app.on_event("startup")
async def startup():
    global redis_client, http_client, kafka_producer
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    http_client = httpx.Client(timeout=10.0)
    
    # Producer para reintentos y DLQ
    conf_producer = {
        'bootstrap.servers': KAFKA_BROKER,
        'client.id': 'cache_service_producer'
    }
    kafka_producer = Producer(conf_producer)
    
    # Verificar conexión Redis
    redis_client.ping()
    print(f"Conectado a Redis en {REDIS_HOST}:{REDIS_PORT}")
    
    # Iniciar hilo del consumidor de Kafka
    threading.Thread(target=run_kafka_consumer, daemon=True).start()
    print("Hilo del consumidor Kafka iniciado en segundo plano.")

@app.on_event("shutdown")
async def shutdown():
    if http_client:
        http_client.close()
    if kafka_producer:
        kafka_producer.flush()

# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------
def build_cache_key(query_type: str, zone_id: str, zone_id_b: Optional[str],
                    confidence_min: float, bins: int) -> str:
    if query_type == "Q1":
        return f"count:{zone_id}:conf={confidence_min}"
    elif query_type == "Q2":
        return f"area:{zone_id}:conf={confidence_min}"
    elif query_type == "Q3":
        return f"density:{zone_id}:conf={confidence_min}"
    elif query_type == "Q4":
        return f"compare:density:{zone_id}:{zone_id_b}:conf={confidence_min}"
    elif query_type == "Q5":
        return f"confidence_dist:{zone_id}:bins={bins}"
    else:
        raw = f"{query_type}:{zone_id}:{zone_id_b}:{confidence_min}:{bins}"
        return hashlib.md5(raw.encode()).hexdigest()

def send_metric(event_type: str, query_type: str, zone_id: str, cache_key: str, latency_ms: float = 0.0):
    try:
        # Enviar métrica de forma síncrona desde el hilo del consumidor
        http_client.post(
            f"{METRICS_URL}/event",
            json={
                "event_type": event_type,
                "query_type": query_type,
                "zone_id": zone_id,
                "cache_key": cache_key,
                "latency_ms": latency_ms,
                "timestamp": time.time(),
            },
            timeout=2.0
        )
    except Exception as e:
        print(f"Error enviando métrica ({event_type}): {e}")

# -------------------------------------------------------------------------
# Hilo del Consumidor de Kafka
# -------------------------------------------------------------------------
def run_kafka_consumer():
    # Pre-crear tópicos Kafka si no existen
    try:
        admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})
        new_topics = [
            NewTopic('consultas_geoespaciales', num_partitions=1, replication_factor=1),
            NewTopic('consultas_retry', num_partitions=1, replication_factor=1),
            NewTopic('consultas_dlq', num_partitions=1, replication_factor=1)
        ]
        fs = admin_client.create_topics(new_topics)
        for topic, f in fs.items():
            try:
                f.result()
                print(f"Tópico '{topic}' creado o verificado.")
            except Exception:
                pass
    except Exception as e:
        print(f"Advertencia al crear tópicos: {e}")

    # Consumer setup
    consumer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': os.getenv("KAFKA_GROUP_ID", "cache_service_group"),
        'auto.offset.reset': os.getenv("KAFKA_OFFSET_RESET", "earliest"),
        'enable.auto.commit': False
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe(['consultas_geoespaciales', 'consultas_retry'])
    
    print("Consumidor Kafka suscrito a 'consultas_geoespaciales' y 'consultas_retry'.")
    lag_recorded = False
    start_recovery_time = None

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        try:
            payload = json.loads(msg.value().decode('utf-8'))
            req = payload.get("req", payload)  # Soporta payloads directo o envueltos
            
            # Asegurar compatibilidad de campos
            query_type = req.get("query_type")
            zone_id = req.get("zone_id")
            zone_id_b = req.get("zone_id_b")
            confidence_min = req.get("confidence_min", 0.0)
            bins = req.get("bins", 5)
            
            cache_key = payload.get("cache_key") or build_cache_key(
                query_type, zone_id, zone_id_b, confidence_min, bins
            )
            t_start = payload.get("t_start") or time.time()
            retries = payload.get("retries", 0)

            # Monitoreo de Lag
            try:
                tp = TopicPartition(msg.topic(), msg.partition())
                low, high = consumer.get_watermark_offsets(tp)
                current_offset = msg.offset()
                lag = high - current_offset - 1
                
                if lag > 0 and not lag_recorded:
                    start_recovery_time = time.time()
                    lag_recorded = True
                elif lag == 0 and lag_recorded and start_recovery_time:
                    recovery_time = time.time() - start_recovery_time
                    send_metric("recovery_time", query_type, zone_id, cache_key, recovery_time * 1000)
                    send_metric("consumer_lag", query_type, zone_id, cache_key, 0)
                    lag_recorded = False
                    start_recovery_time = None

                if lag > 0 and lag % 10 == 0:
                    send_metric("consumer_lag", query_type, zone_id, cache_key, lag)
            except Exception:
                pass

            # 1. Verificar Redis (Cache Hit)
            cached = redis_client.get(cache_key)
            if cached is not None:
                latency_ms = (time.time() - t_start) * 1000
                print(f"  [CACHE HIT] Key: {cache_key} | Latency: {latency_ms:.2f}ms")
                send_metric("hit", query_type, zone_id, cache_key, latency_ms)
                consumer.commit(msg)
                continue

            # 2. Cache Miss: Derivar al Response Generator vía HTTP
            try:
                resp = http_client.post(f"{RESPONSE_GEN_URL}/query", json=req)
                resp.raise_for_status()
                response_data = resp.json()
                result = response_data["result"]
                
                # Escribir en caché
                redis_client.setex(cache_key, CACHE_TTL, json.dumps(result))
                
                latency_ms = (time.time() - t_start) * 1000
                if retries > 0:
                    print(f"  [RECOVERY SUCCESS] Key: {cache_key} | Latency: {latency_ms:.2f}ms | Retries: {retries}")
                    send_metric("recovery", query_type, zone_id, cache_key, latency_ms)
                else:
                    print(f"  [CACHE MISS] Key: {cache_key} | Latency: {latency_ms:.2f}ms")
                    send_metric("miss", query_type, zone_id, cache_key, latency_ms)
                
                consumer.commit(msg)

            except Exception as e:
                # Ocurrió una falla temporal en Response Generator
                print(f"Falla temporal llamando al generador de respuestas: {e}")
                retries += 1
                payload["retries"] = retries
                payload["t_start"] = t_start
                payload["cache_key"] = cache_key
                payload["req"] = req

                if retries == 1:
                    # Registrar como un miss inicial la primera vez que falla
                    send_metric("miss", query_type, zone_id, cache_key, (time.time() - t_start) * 1000)

                if retries <= 3:
                    print(f"Reintentando consulta {cache_key} (Intento {retries})...")
                    kafka_producer.produce('consultas_retry', value=json.dumps(payload).encode('utf-8'))
                    send_metric("retry", query_type, zone_id, cache_key)
                else:
                    print(f"Consulta {cache_key} excedió el límite de reintentos. Despachando a DLQ.")
                    kafka_producer.produce('consultas_dlq', value=json.dumps(payload).encode('utf-8'))
                    send_metric("dlq", query_type, zone_id, cache_key)

                kafka_producer.poll(0)
                consumer.commit(msg)

        except Exception as e:
            print(f"Error inesperado procesando mensaje: {e}")
            consumer.commit(msg)

# -------------------------------------------------------------------------
# Endpoints FastAPI
# -------------------------------------------------------------------------
@app.get("/health")
def health():
    try:
        redis_client.ping()
        redis_status = "ok"
    except Exception:
        redis_status = "error"
    return {"status": "ok", "redis": redis_status}

@app.get("/stats")
def get_redis_stats():
    """Retorna estadísticas internas de Redis (hits, misses, evictions)."""
    info = redis_client.info("stats")
    memory = redis_client.info("memory")
    return {
        "keyspace_hits": info.get("keyspace_hits", 0),
        "keyspace_misses": info.get("keyspace_misses", 0),
        "evicted_keys": info.get("evicted_keys", 0),
        "used_memory_human": memory.get("used_memory_human", "N/A"),
        "maxmemory_human": memory.get("maxmemory_human", "N/A"),
        "maxmemory_policy": memory.get("maxmemory_policy", "N/A"),
    }
