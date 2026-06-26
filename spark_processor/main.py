import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime, window, when, sum as _sum, count, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "elasticsearch")
ELASTICSEARCH_PORT = os.getenv("ELASTICSEARCH_PORT", "9200")

def main():
    spark = SparkSession.builder \
        .appName("MetricsProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1") \
        .config("spark.es.nodes", ELASTICSEARCH_HOST) \
        .config("spark.es.port", ELASTICSEARCH_PORT) \
        .config("spark.es.nodes.wan.only", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Esquema de los mensajes JSON en Kafka
    schema = StructType([
        StructField("timestamp", DoubleType(), True),
        StructField("event_type", StringType(), True),
        StructField("query_type", StringType(), True),
        StructField("zone_id", StringType(), True),
        StructField("cache_key", StringType(), True),
        StructField("latency_ms", DoubleType(), True)
    ])

    # Leer de Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", "metrics-topic") \
        .option("startingOffsets", "latest") \
        .load()

    # Parsear JSON y convertir timestamp
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_time", from_unixtime(col("timestamp")).cast("timestamp"))

    # Filtrar eventos no válidos si los hay
    valid_df = parsed_df.filter(col("event_time").isNotNull())

    # Ventanas deslizantes de 1 minuto, actualizadas cada 10 segundos
    windowed_aggs = valid_df \
        .withWatermark("event_time", "1 minute") \
        .groupBy(window(col("event_time"), "1 minute", "10 seconds")) \
        .agg(
            # Total de consultas exitosas (hits + misses)
            _sum(when(col("event_type").isin("hit", "miss"), 1).otherwise(0)).alias("total_requests"),
            
            # Throughput por minuto (ya que la ventana es de 1 minuto)
            _sum(when(col("event_type").isin("hit", "miss"), 1).otherwise(0)).alias("throughput_per_min"),
            
            # Latencias p50 y p95
            expr("percentile_approx(latency_ms, 0.5)").alias("latency_p50"),
            expr("percentile_approx(latency_ms, 0.95)").alias("latency_p95"),
            
            # Hit Rate
            (_sum(when(col("event_type") == "hit", 1).otherwise(0)) / 
             _sum(when(col("event_type").isin("hit", "miss"), 1).otherwise(1))).alias("hit_rate"),
            
            # Retry Rate
            (_sum(when(col("event_type") == "retry", 1).otherwise(0)) / 
             _sum(when(col("event_type").isin("hit", "miss"), 1).otherwise(1))).alias("retry_rate")
        )

    es_df = windowed_aggs.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_requests"),
        col("throughput_per_min"),
        col("latency_p50"),
        col("latency_p95"),
        col("hit_rate"),
        col("retry_rate")
    ).withColumn("@timestamp", col("window_end")) \
     .withColumn("doc_id", col("window_start").cast("string"))

    # Escribir en Elasticsearch
    query = es_df.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/spark-checkpoints") \
        .option("es.mapping.id", "doc_id") \
        .option("es.resource", "metrics-index") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
