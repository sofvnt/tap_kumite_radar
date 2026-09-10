# 🥋 Kumite Radar - Real-Time Sports Analytics

## 📖 Descrizione del Progetto
Progetto finale per il corso di **Technologies for Advanced Programming** (TAP) - Università degli Studi di Catania.

"Kumite Radar" è una pipeline architetturale di stream processing progettata per l'analisi tattica in tempo reale degli incontri di Karate (specialità Kumite). Il sistema traccia i movimenti degli atleti (AKA e AO) tramite Computer Vision (YOLOv8) e utilizza algoritmi di Machine Learning in streaming per classificare dinamicamente le fasi del combattimento (es. "Fase di Studio" vs "Fase di Ingaggio").

## 🏗️ Architettura e Tecnologie
L'intera infrastruttura è a microservizi, orchestrata tramite **Docker Compose** e progettata per garantire fault-tolerance e disaccoppiamento tra produzione, calcolo e visualizzazione dei dati.

* **Computer Vision (Source):** YOLOv8 + OpenCV
* **Data Ingestion & Buffering:** Apache Kafka (KRaft mode) + Python Custom Producer
* **Real-Time Processing & ML:** Apache Spark (Structured Streaming) + Spark MLlib 
* **Indexing & Storage:** Elasticsearch
* **Data Visualization:** Kibana

## ⭐ Requisiti "Plus" Soddisfatti
In linea con la griglia di valutazione del corso, il progetto include le seguenti implementazioni avanzate:
1. **Machine Learning in Streaming:** Applicazione del clustering **K-Means** (MLlib) in tempo reale su micro-batch (Tumbling Windows da 3 secondi) per classificare dinamicamente le fasi del match senza regole prefissate.
2. **Ecosistema Docker:** Architettura interamente containerizzata con risoluzione DNS interna, gestione coordinata del fuso orario (`TZ=Europe/Rome`) e *version pinning* rigoroso dei container.



## 🚀 Guida all'Avvio (Quickstart)

Per eseguire il progetto, è necessario avere **Docker** installato sul proprio computer e un ambiente Python locale per la Computer Vision (`ultralytics`, `opencv-python`, `kafka-python`).

Aprire tre terminali distinti nella cartella principale del progetto ed eseguire i comandi in rigorosa sequenza:

### Terminale 1: Infrastruttura e Database
Avvia l'ecosistema dei container (Kafka, Elasticsearch, Kibana, Spark) in background:
```bash
docker compose up -d

```

*Attendere circa 30-40 secondi affinché i servizi completino l'avvio e comunichino tra loro.*

### Terminale 2: Ingestion & Tracciamento Video (Producer)

Avvia lo script di tracciamento YOLOv8. Cattura il video, estrae le coordinate e le invia in tempo reale al topic Kafka:

```bash
python tracker.py

```

*Lasciare l'interfaccia video in esecuzione.*

### Terminale 3: Stream Processing & Machine Learning (Spark)

Esegue il job di PySpark all'interno del container dedicato. Il comando installa le dipendenze matematiche (`numpy`), scarica i connettori necessari per Kafka ed Elasticsearch e avvia l'aggregazione a finestre e il K-Means:

```bash
docker exec -w /app tap-spark bash -c "pip install numpy && /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 spark_processor.py"

```

## 📊 Visualizzazione dei Risultati

Appena il terminale di Spark inizia a processare i micro-batch, la dashboard si animerà automaticamente:

1. Aprire il browser all'indirizzo **http://localhost:5601** (Kibana).
2. Impostare il filtro temporale in alto a destra su **"Last 15 minutes"** (oppure **"Today"**).
3. I dati affluiranno in tempo reale mostrando la classificazione dinamica delle fasi del match generata dall'algoritmo non supervisionato.
