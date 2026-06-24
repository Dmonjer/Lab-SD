#!/usr/bin/env python3
"""
run_experiments.py - Script de control para ejecutar los experimentos de análisis.

Corre automáticamente los experimentos requeridos por la tarea:
  1. Distribución Zipf vs Uniforme (con LRU, 200MB, TTL=300)
  2. Políticas de evicción: LRU, LFU, FIFO (con Zipf, 200MB)
  3. Tamaño de caché: 50MB, 200MB, 500MB (con LRU, Zipf)
  4. TTL: 60s, 300s, 3600s (con LRU, 200MB, Zipf)

Al terminar, exporta los resultados combinados en results/experiments_summary.csv
"""
import sys
import time
import json
import subprocess
import httpx
import csv
from pathlib import Path

CACHE_URL = "http://localhost:8002"
TRAFFIC_URL = "http://localhost:8004"
METRICS_URL = "http://localhost:8003"
RESULTS_DIR = Path("results")

N_REQUESTS = 500
REQUEST_RATE = 20.0
ZIPF_ALPHA = 1.5


def wait_for_service(url: str, name: str, retries: int = 30):
    print(f"Esperando que {name} esté listo...")
    for i in range(retries):
        try:
            r = httpx.get(f"{url}/health", timeout=3.0)
            if r.status_code == 200:
                print(f"  [OK] {name} listo.")
                return True
        except Exception:
            pass
        time.sleep(2)
    print(f"  [FAIL] {name} no respondió.")
    return False


def reset_metrics():
    try:
        httpx.delete(f"{METRICS_URL}/reset", timeout=5.0)
    except Exception:
        pass


def run_traffic(distribution: str, n_requests: int = N_REQUESTS, alpha: float = ZIPF_ALPHA):
    """Lanza el generador de tráfico y espera a que termine."""
    print(f"  Ejecutando tráfico: dist={distribution}, n={n_requests}, alpha={alpha}")
    r = httpx.post(
        f"{TRAFFIC_URL}/run",
        json={"n_requests": n_requests, "distribution": distribution, "zipf_alpha": alpha, "request_rate": REQUEST_RATE},
        timeout=30.0,
    )
    r.raise_for_status()

    # Esperar a que termine
    while True:
        status = httpx.get(f"{TRAFFIC_URL}/status", timeout=30.0).json()
        if not status["is_running"]:
            break
        time.sleep(3)

    # Esperar a que el consumidor de Kafka procese todos los mensajes (total_requests en metrics == n_requests)
    print("  Esperando a que se procesen todos los mensajes en Kafka...")
    start_wait = time.time()
    while time.time() - start_wait < 120: # Max 120 seconds timeout
        try:
            stats = get_metrics_stats()
            if stats["total_requests"] >= n_requests:
                break
        except Exception:
            pass
        time.sleep(1)

    time.sleep(2)  # dar tiempo a que las métricas se escriban


def get_cache_stats():
    r = httpx.get(f"{CACHE_URL}/stats", timeout=5.0)
    return r.json()


def get_metrics_stats():
    r = httpx.get(f"{METRICS_URL}/stats", timeout=5.0)
    return r.json()


def restart_redis(maxmemory: str, policy: str, ttl: int = 300):
    """Reinicia el contenedor redis con nueva configuración."""
    print(f"  Reiniciando Redis: maxmemory={maxmemory}, policy={policy}")
    subprocess.run(
        ["docker", "compose", "stop", "redis"],
        cwd=".", capture_output=True, check=True
    )

    # Generar un group.id único para evitar que el broker espere por rebalanceos de consumidores viejos
    group_id = f"group_{int(time.time())}"

    # Actualizar .env temporalmente
    env_content = f"""CACHE_MAX_MEMORY={maxmemory}
CACHE_EVICTION_POLICY={policy}
CACHE_TTL={ttl}
PROCESSING_DELAY_MS=100
DISTRIBUTION=zipf
N_REQUESTS={N_REQUESTS}
ZIPF_ALPHA={ZIPF_ALPHA}
REQUEST_RATE={REQUEST_RATE}
KAFKA_GROUP_ID={group_id}
KAFKA_OFFSET_RESET=latest
"""
    with open(".env", "w") as f:
        f.write(env_content)

    subprocess.run(
        ["docker", "compose", "up", "-d", "redis"],
        cwd=".", capture_output=True, check=True
    )
    time.sleep(3)

    # Recrear el cache_service y consumer_worker para aplicar el nuevo TTL y Group ID
    subprocess.run(
        ["docker", "compose", "up", "-d", "cache_service", "consumer_worker"],
        cwd=".", capture_output=True, check=True
    )
    # Esperar a que los consumidores se conecten y suscriban
    print("  Esperando 15s a que los consumidores se suscriban a Kafka...")
    time.sleep(15)


def save_result(results: list, filename: str):
    RESULTS_DIR.mkdir(exist_ok=True)
    if not results:
        return
    keys = list(results[0].keys())
    path = RESULTS_DIR / filename
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(results)
    print(f"  Resultados guardados en {path}")


def main():
    RESULTS_DIR.mkdir(exist_ok=True)

    # Verificar que todos los servicios estén activos
    if not all([
        wait_for_service(CACHE_URL, "Cache Service"),
        wait_for_service(TRAFFIC_URL, "Traffic Generator"),
        wait_for_service(METRICS_URL, "Metrics Storage"),
    ]):
        print("ERROR: No todos los servicios están disponibles. Ejecuta 'docker compose up -d' primero.")
        sys.exit(1)

    all_results = []

    # =====================================================================
    # Experimento 1: Zipf vs Uniforme
    # =====================================================================
    print("\n" + "="*60)
    print("EXPERIMENTO 1: Distribución de Tráfico (Zipf vs Uniforme)")
    print("="*60)
    for dist in ["zipf", "uniform"]:
        restart_redis("200mb", "allkeys-lru")
        reset_metrics()
        run_traffic(dist)
        stats = get_metrics_stats()
        cache_stats = get_cache_stats()
        row = {
            "experiment": "dist_comparison",
            "distribution": dist,
            "eviction_policy": "allkeys-lru",
            "maxmemory": "200mb",
            "ttl": 300,
            "hit_rate": stats["hit_rate"],
            "total_hits": stats["total_hits"],
            "total_misses": stats["total_misses"],
            "latency_p50_ms": stats["latency_p50_ms"],
            "latency_p95_ms": stats["latency_p95_ms"],
            "evicted_keys": cache_stats.get("evicted_keys", 0),
        }
        print(f"  {dist}: hit_rate={row['hit_rate']}%, p50={row['latency_p50_ms']}ms, p95={row['latency_p95_ms']}ms")
        all_results.append(row)

    # =====================================================================
    # Experimento 2: Políticas de Evicción (LRU, LFU, FIFO)
    # =====================================================================
    print("\n" + "="*60)
    print("EXPERIMENTO 2: Políticas de Evicción")
    print("="*60)

    policies = [
        ("allkeys-lru", "LRU"),
        ("allkeys-lfu", "LFU"),
        ("allkeys-random", "FIFO/Random"),  # Redis no tiene FIFO puro, random es el equivalente más cercano
    ]

    for policy_key, policy_name in policies:
        restart_redis("200mb", policy_key)
        reset_metrics()
        run_traffic("zipf")
        stats = get_metrics_stats()
        cache_stats = get_cache_stats()
        row = {
            "experiment": "policy_comparison",
            "distribution": "zipf",
            "eviction_policy": policy_name,
            "maxmemory": "200mb",
            "ttl": 300,
            "hit_rate": stats["hit_rate"],
            "total_hits": stats["total_hits"],
            "total_misses": stats["total_misses"],
            "latency_p50_ms": stats["latency_p50_ms"],
            "latency_p95_ms": stats["latency_p95_ms"],
            "evicted_keys": cache_stats.get("evicted_keys", 0),
        }
        print(f"  {policy_name}: hit_rate={row['hit_rate']}%, evictions={row['evicted_keys']}")
        all_results.append(row)

    # =====================================================================
    # Experimento 3: Tamaño de Caché (50MB, 200MB, 500MB)
    # =====================================================================
    print("\n" + "="*60)
    print("EXPERIMENTO 3: Tamaño de Caché")
    print("="*60)

    for size in ["50mb", "200mb", "500mb"]:
        restart_redis(size, "allkeys-lru")
        reset_metrics()
        run_traffic("zipf")
        stats = get_metrics_stats()
        cache_stats = get_cache_stats()
        row = {
            "experiment": "size_comparison",
            "distribution": "zipf",
            "eviction_policy": "LRU",
            "maxmemory": size,
            "ttl": 300,
            "hit_rate": stats["hit_rate"],
            "total_hits": stats["total_hits"],
            "total_misses": stats["total_misses"],
            "latency_p50_ms": stats["latency_p50_ms"],
            "latency_p95_ms": stats["latency_p95_ms"],
            "evicted_keys": cache_stats.get("evicted_keys", 0),
        }
        print(f"  {size}: hit_rate={row['hit_rate']}%, evictions={row['evicted_keys']}")
        all_results.append(row)

    # =====================================================================
    # Experimento 4: TTL (60s, 300s, 3600s)
    # =====================================================================
    print("\n" + "="*60)
    print("EXPERIMENTO 4: TTL")
    print("="*60)

    for ttl in [30, 60, 120]:
        restart_redis("200mb", "allkeys-lru", ttl)
        reset_metrics()
        # Ejecutar tráfico en dos rondas para observar expiración
        run_traffic("zipf")
        print(f"  Esperando {ttl // 2}s para observar expiración de caché...")
        time.sleep(ttl // 2)  
        run_traffic("zipf")
        stats = get_metrics_stats()
        cache_stats = get_cache_stats()
        row = {
            "experiment": "ttl_comparison",
            "distribution": "zipf",
            "eviction_policy": "LRU",
            "maxmemory": "200mb",
            "ttl": ttl,
            "hit_rate": stats["hit_rate"],
            "total_hits": stats["total_hits"],
            "total_misses": stats["total_misses"],
            "latency_p50_ms": stats["latency_p50_ms"],
            "latency_p95_ms": stats["latency_p95_ms"],
            "evicted_keys": cache_stats.get("evicted_keys", 0),
        }
        print(f"  TTL={ttl}s: hit_rate={row['hit_rate']}%")
        all_results.append(row)

    # =====================================================================
    # Guardar resultados
    # =====================================================================
    save_result(all_results, "experiments_summary.csv")
    print(f"\n[DONE] Todos los experimentos completados. Resultados en {RESULTS_DIR}/experiments_summary.csv")
    print("  Ejecuta 'python analyze.py' para generar los gráficos.")


if __name__ == "__main__":
    main()
