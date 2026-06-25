# 🥋 Kumite Analytics - Real-Time Computer Vision & Big Data

Questo progetto implementa una pipeline Big Data in tempo reale per l'analisi tattica del Karate sportivo (Kumite).

## 🏗️ Architettura
L'infrastruttura è interamente dockerizzata e sfrutta un'architettura **Edge-to-Broker**:
1. **Edge (Ingestion):** `tracker.py` utilizza **YOLOv8** per estrarre i bounding boxes degli atleti da un flusso video e funge da Producer inviando i dati spaziali in JSON direttamente a Kafka (30 fps).
2. **Streaming & Processing:** Apache Spark (Structured Streaming) consuma i messaggi da Kafka, calcola la distanza tra i lottatori e il **Dominio del Centro** in tempo reale.
3. **Storage & Data Viz:** I dati arricchiti vengono indicizzati su Elasticsearch e visualizzati dinamicamente tramite una dashboard in streaming su Kibana.

## 🚀 Come avviare il progetto

**1. Avviare l'infrastruttura Docker**
```bash
docker compose up -d



2. Avviare la pipeline Spark
Attendere che i container siano attivi, quindi lanciare:

docker exec -w /app tap-spark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.12.0 spark_processor.py



3. Avviare l'ingestion video (YOLO)
In un terminale separato locale, avviare:

python tracker.py



4. Visualizzazione
Aprire Kibana all'indirizzo http://localhost:5601 e visualizzare i dati del kumite_index.