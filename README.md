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
