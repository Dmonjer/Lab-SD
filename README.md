# Tarea 3 SD - Streaming y Análisis de Métricas con Apache Spark y ELK Stack

Esta iteración (Tarea 3) expande la arquitectura altamente desacoplada de la Tarea 2, añadiendo un pipeline de **Procesamiento de Streaming en Tiempo Real** para monitoreo y analítica operativa del sistema.

Se introdujo **Apache Spark (Structured Streaming)** para procesar eventos en vivo desde Kafka, y el **ELK Stack (Elasticsearch + Kibana)** para indexar y visualizar estas métricas agregadas mediante ventanas de tiempo.

---

## Guía de Revisión para el Ayudante Corrector

Para revisar esta tarea y comprobar el correcto funcionamiento del pipeline de Big Data, por favor sigue estos pasos ordenados:

### 1. Levantar la Infraestructura Completa
Levanta todos los servicios (Generadores, Caché, Kafka, Zookeeper, Spark, Elasticsearch y Kibana) con un solo comando.  
*(Nota: Si es primera vez, tomará algunos minutos descargar las imágenes oficiales de ELK y la imagen base de Spark con Java).*
```bash
docker compose up --build -d
```
Espera un momento a que todos los contenedores reporten "Started" y estén listos. Gracias al script interno del `docker-compose`, el tópico necesario en Kafka se creará de forma automática y segura antes de encender Spark.

### 2. Generar Tráfico y Alimentar el Pipeline
Para que Spark tenga datos que procesar y graficar en Elasticsearch, debemos simular tráfico en el sistema usando el script de pruebas automatizadas:
```bash
python run_experiments.py
```
> **Detalle Importante:** Este script tomará alrededor de **5 a 6 minutos** en completar todas sus fases, ya que inserta esperas intencionales para evaluar las caducidades del TTL. 
> Durante este tiempo, los contenedores de la app estarán produciendo eventos, Kafka los encolará en su tópico y `spark_processor` procesará las métricas agrupándolas en ventanas de 1 minuto, enviando los agregados a Elasticsearch.

### 3. Verificar los Datos Visualmente en Kibana
Una vez que el script de tráfico haya avanzado un par de minutos (o haya terminado), puedes ingresar al panel de visualización:
1. Abre tu navegador web en **[http://localhost:5601](http://localhost:5601)**
2. En el menú hamburguesa de Kibana, navega a **Stack Management > Data Views** (o Index Patterns).
3. Haz clic en **Create data view**. Como patrón de índice (Index pattern), escribe: `metrics-index`. Si los datos llegaron bien, Kibana detectará el índice automáticamente a la derecha.
4. Selecciona `@timestamp` como el campo de tiempo (Time field) y guarda.
5. Dirígete a la sección **Discover** en el menú principal lateral.
6. **¡Listo!** Deberías ver los documentos pre-agregados por Spark en tiempo real (mostrando campos analíticos ya condensados como `hit_rate`, `throughput_per_min`, `latency_p50`, etc.) listos para armar cuadros de mando interactivos.

### 4. Consultar Elasticsearch Directamente (Opcional)
Si deseas comprobar la existencia de los datos por terminal sin tener que ingresar a la interfaz gráfica de Kibana, basta con lanzar:
```bash
curl -X GET "localhost:9200/metrics-index/_search?pretty"
```

---

## Arquitectura del Sistema Expandida

Se añadió un segundo pipeline de datos enfocado 100% en la telemetría, el cual no impacta las latencias de los usuarios.

```text
[Traffic Generator] & [Cache/Workers]
          │
          ▼ (Emite Eventos: HITS, MISS, Latencias)
┌──────────────────────┐
│   Metrics Storage    │ (Actúa como Kafka Producer de telemetría)
└──────────────────────┘
          │
          ▼ 
┌───────────────────────┐
│ Apache Kafka (Broker) │ ──► (Tópico: metrics-topic)
└───────────────────────┘
          │
          ▼ (Suscripción Streaming)
┌───────────────────────┐
│    Spark Processor    │ (PySpark Structured Streaming)
└───────────────────────┘
          │
          ▼ (Output Mode: Update/Upsert)
┌───────────────────────┐
│    Elasticsearch      │ (Índice: metrics-index)
└───────────────────────┘
          │
          ▼ (Data Views y Discover)
┌───────────────────────┐
│       Kibana          │ (Port 5601)
└───────────────────────┘
```

---

## Endpoints Principales

| Servicio | Puerto Host | Enlace Local |
|---|---|---|
| **Kibana (Dashboard)** | `5601` | [Panel de Control ELK](http://localhost:5601) |
| **Elasticsearch** | `9200` | [API REST ES](http://localhost:9200) |
| **Response Generator** | `8001` | [Documentación Swagger](http://localhost:8001/docs) |
| **Broker de Kafka** | `9092` | `localhost:9092` |
| **Redis** | `6379` | `localhost:6379` |
