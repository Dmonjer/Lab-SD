# Tarea 2 SD - Procesamiento Asíncrono y Fallback con Apache Kafka y Redis

Esta versión implementa una arquitectura distribuida asíncrona y tolerante a fallos para el análisis de consultas geoespaciales sobre el dataset **Google Open Buildings (Santiago)**. Se introduce **Apache Kafka** como bus de eventos para desacoplar el Generador de Tráfico de los procesadores, implementando colas de reintentos y una cola de mensajes descartados (Dead Letter Queue - DLQ).

---

## Arquitectura del Sistema

La comunicación entre el Generador de Tráfico y los procesadores se realiza de forma 100% asíncrona utilizando tópicos de Kafka:

```
                  ┌──────────────────────┐
                  │ Generador de Tráfico │
                  │       (FastAPI)      │
                  └──────────────────────┘
                              │
                              │ (Publica asíncronamente)
                              ▼
                      ┌───────────────┐
                      │ Apache Kafka  │◀──────────────────────────────┐
                      └───────────────┘                               │
                        │ (Consume)                                   │
                        ▼                                             │
┌──────────────────────────────────────────────────────────────────┐  │
│                  Cache Service / Consumer Worker                 │  │ (Reintentos / DLQ)
│                                                                  │  │
│  ┌─────────────────────────────┐    ┌─────────────────────────┐  │──┘
│  │   FastAPI Server (Stats)    │    │  Kafka Consumer Thread  │  │
│  │         Port 8002           │    │    (Poll de Eventos)    │  │
│  └─────────────────────────────┘    └─────────────────────────┘  │
└──────────────────┬───────────────────────────────┬───────────────┘
                   │                               │
                   │ (Verifica / Escribe Cache)    │ (Miss - HTTP POST)
                   ▼                               ▼
            ┌─────────────┐             ┌──────────────────────┐
            │    Redis    │             │  Response Generator  │
            │  Port 6379  │             │      Port 8001       │
            └─────────────┘             └──────────────────────┘
                                                   │
                                                   │ (Métricas)
                                                   ▼
                                        ┌──────────────────────┐
                                        │   Metrics Storage    │
                                        │      Port 8003       │
                                        └──────────────────────┘
```

---

## Flujo de Mensajería y Tolerancia a Fallos

1. **Flujo Principal:** El *Traffic Generator* encola las consultas en el tópico `consultas_geoespaciales`. El worker procesa la cola: si hay *Cache Hit* en Redis finaliza, si hay *Cache Miss* hace un POST al *Response Generator*, guarda el resultado en Redis y confirma el offset.
2. **Política de Reintentos:** Si el *Response Generator* sufre una caída o está sobrecargado, el worker captura el error de conexión y reenvía el mensaje al tópico `consultas_retry`.
3. **Dead Letter Queue (DLQ):** Cada mensaje fallido incrementa un contador de reintentos. Si una consulta falla por cuarta vez (superando los 3 reintentos límite), se envía al tópico `consultas_dlq` y se marca como descartada para no bloquear la cola principal.

---

## Stack Tecnológico

| Componente | Tecnología | Rol / Justificación |
|---|---|---|
| **Mensajería** | Apache Kafka 7.5 (Confluent) | Bus de eventos distribuido, persistente y escalable |
| **Coordinador** | ZooKeeper | Gestión y coordinación del clúster de Kafka |
| **Base de Datos / Caché** | Redis 7 | Almacenamiento clave-valor en memoria con políticas de desalojo (LRU/LFU/FIFO) |
| **Backend Analítico** | Python + FastAPI + Pandas | Carga el dataset Parquet en memoria RAM y ejecuta operaciones vectoriales (Q1-Q5) |
| **Worker / Caché** | Python + confluent-kafka + Redis | Consumidor multihilo que interactúa con Redis, Kafka y el Generador |
| **Métricas** | FastAPI + CSV + Matplotlib | Almacenamiento cronológico de eventos y generación de reportes gráficos |

---

## Requisitos Previos

- **Docker Desktop** (con soporte para Docker Compose v2)
- **Python 3.11+** (para ejecutar scripts locales de control y análisis)
- Librerías Python necesarias:
  ```bash
  pip install httpx pandas matplotlib pyarrow s2sphere
  ```

---

## Puesta en Marcha y Demostración en Vivo

### 1. Levantar la Infraestructura Completa
Levanta todos los servicios en segundo plano:
```bash
docker compose up -d
```

### 2. Comandos Secuenciales de Prueba (Terminal Izquierda)

Puedes seguir los siguientes pasos para replicar la demo en vivo:

```bash
# Paso 1: Mostrar estado inicial de los contenedores
docker compose ps

# Paso 2: Inyectar ráfaga inicial de 10 peticiones (Distribución Zipf)
Invoke-RestMethod -Method Post -Uri http://localhost:8004/run -ContentType "application/json" -Body '{"n_requests": 10, "distribution": "zipf"}'

# Paso 3: Simular caída deteniendo el contenedor del generador de respuestas
docker compose stop response_generator

# Paso 4: Enviar 5 peticiones con el motor apagado (Verás reintentos e ingresos a la DLQ en los logs)
Invoke-RestMethod -Method Post -Uri http://localhost:8004/run -ContentType "application/json" -Body '{"n_requests": 5, "distribution": "zipf"}'

# Paso 5: Volver a prender el generador (Recuperación)
docker compose start response_generator

# (Espera 15 segundos a que cargue el dataset Parquet en memoria antes del Paso 6)

# Paso 6: Enviar 1 petición (Comprobar recuperación del servicio)
Invoke-RestMethod -Method Post -Uri http://localhost:8004/run -ContentType "application/json" -Body '{"n_requests": 1, "distribution": "zipf"}'

# Paso 7: Escalar los workers de consumo de 1 a 3 instancias en paralelo
docker compose up -d --scale consumer_worker=3

# Paso 8: Comprobar el estado de las 3 réplicas concurrentes
docker compose ps
```

---

## Ejecución de Experimentos Automáticos

Para correr los experimentos sistemáticos de la tarea y generar los reportes analíticos:

```bash
# 1. Ejecutar el script que automatiza los experimentos (Zipf vs Uniforme, políticas de evicción, etc.)
python run_experiments.py

# 2. Copiar el log detallado de métricas acumulado
docker cp metrics_storage:/metrics/events.csv results/events.csv

# 3. Graficar los resultados del informe
python analyze.py
```
Los gráficos finales se guardarán en `results/figures/`.

---

## Endpoints de los Servicios

| Servicio | Puerto Host | Enlace Local |
|---|---|---|
| **Response Generator** | `8001` | [Documentación Swagger](http://localhost:8001/docs) |
| **Cache Service / Stats** | `8002` | [Estadísticas Redis](http://localhost:8002/stats) |
| **Metrics Storage** | `8003` | [Métricas Dashboard](http://localhost:8003/stats) |
| **Traffic Generator** | `8004` | [Controlador Tráfico](http://localhost:8004/docs) |
| **Broker de Kafka** | `9092` | `localhost:9092` |
| **Redis** | `6379` | `localhost:6379` |

---

## Variables de Entorno del Archivo `.env`

* `CACHE_MAX_MEMORY`: Límite de RAM de Redis (ej. `200mb`).
* `CACHE_EVICTION_POLICY`: Política de reemplazo (ej. `allkeys-lru`, `allkeys-lfu`, `noeviction`).
* `CACHE_TTL`: Tiempo de vida en segundos de las llaves en caché.
* `PROCESSING_DELAY_MS`: Latencia artificial en el generador (ej. `100ms`).
* `KAFKA_GROUP_ID`: Identificador de grupo para los consumidores.
* `KAFKA_OFFSET_RESET`: Política de lectura de offsets (`latest` / `earliest`).
