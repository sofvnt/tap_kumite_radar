import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sqrt, pow, lit, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# ── CONFIGURAZIONE CENTRO TATAMI ─────────────────────────────────────────────
# Coordinate del centro dell'inquadratura (es. per video 640x360)
CENTRO_X = 320
CENTRO_Y = 180

print("=======================================================")
print("  Avvio Spark Structured Streaming - Kumite Analytics  ")
print("=======================================================")

# 1. Inizializzazione della Sessione Spark con i connettori Kafka ed Elasticsearch
spark = SparkSession.builder \
    .appName("KumiteCenterDominance") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1") \
    .config("spark.es.nodes", "localhost") \
    .config("spark.es.port", "9200") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Definizione dello Schema del JSON in arrivo da Kafka
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("atleta_aka_x", IntegerType(), True),
    StructField("atleta_aka_y", IntegerType(), True),
    StructField("atleta_ao_x", IntegerType(), True),
    StructField("atleta_ao_y", IntegerType(), True),
    StructField("distanza", DoubleType(), True)
])

# 3. Lettura del flusso in tempo real da Apache Kafka
print("[INFO] Connessione al topic Kafka 'kumite_topic'...")
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "kumite_topic") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Decodifica del codice in binario di Kafka in stringa e applicazione dello schema
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json_val") \
    .select(from_json(col("json_val"), schema).alias("data")) \
    .select("data.*")

# 5. Calcolo delle distanze dal centro del tatami
stream_con_metriche = parsed_stream \
    .withColumn("dist_aka_centro", sqrt(pow(col("atleta_aka_x") - lit(CENTRO_X), 2) + pow(col("atleta_aka_y") - lit(CENTRO_Y), 2))) \
    .withColumn("dist_ao_centro", sqrt(pow(col("atleta_ao_x") - lit(CENTRO_X), 2) + pow(col("atleta_ao_y") - lit(CENTRO_Y), 2)))

# 6. Determinazione di chi domina il centro (chi è più vicino al centro ha una distanza minore)
stream_finale = stream_con_metriche.withColumn(
    "dominio_centro",
    when(col("dist_aka_centro") < col("dist_ao_centro"), "AKA")
    .when(col("dist_aka_centro") > col("dist_ao_centro"), "AO")
    .otherwise("NEUTRO")
)

# 7. Scrittura del flusso elaborato direttamente su Elasticsearch
print("[INFO] Pipeline Spark attivata. Scrittura continua su 'kumite_index'...")
query = stream_finale.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("checkpointLocation", "/app/checkpoint_spark_kumite") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .option("es.resource", "kumite_index") \
    .option("es.index.auto.create", "true") \
    .outputMode("append") \
    .start()

query.awaitTermination()

# Rimane in ascolto del flusso continuo
query.awaitTermination()