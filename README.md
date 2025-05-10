# Diabetes Prediction MLOps Pipeline

This repository contains an end-to-end MLOps pipeline for diabetes prediction using a modern data architecture that includes batch processing, real-time streaming, data validation, and model deployment.

## Architecture Overview

<img width="883" alt="Ảnh màn hình 2025-05-10 lúc 19 14 54" src="https://github.com/user-attachments/assets/dbd07edf-8149-426f-b451-31ccf20fe2b9" />



### Components

- **MinIO**: Object storage for storing raw diabetes datasets.
- **Apache Spark**: Performs batch processing on data stored in MinIO.
- **PostgreSQL**: Acts as a data warehouse for processed data.
- **Great Expectations**: Validates the quality and schema of processed data.
- **Apache Airflow**: Manages scheduled jobs like data processing and model retraining.
- **Debezium**: Monitors changes in PostgreSQL and streams them to Kafka.
- **Apache Kafka**: A messaging queue to stream data between services.
- **Apache Flink**: Performs real-time stream processing and validation.
- **FastAPI**: Serves a prediction API for diabetes inference.
- **Serving Table**: Final table used for inference and model retraining.

---

## Workflow

1. **Data Ingestion**  
   Raw data is uploaded to MinIO.  
   Spark processes the data and writes it to PostgreSQL.

2. **Data Validation**  
   Great Expectations validates the processed data before updating the serving table.

3. **CDC & Streaming**  
   Debezium detects new data in PostgreSQL and sends it to Kafka.  
   Flink consumes Kafka streams, processes and validates real-time data.

4. **Prediction Service**  
   FastAPI service consumes validated data and returns predictions.

5. **Model Retraining**  
   Airflow schedules periodic jobs to retrain the model using updated data.

---

## Tech Stack

- **Storage**: MinIO, PostgreSQL
- **Batch Processing**: Apache Spark
- **Stream Processing**: Apache Flink, Kafka, Debezium
- **Data Validation**: Great Expectations
- **Workflow Orchestration**: Apache Airflow
- **Model Serving**: FastAPI
- **Language**: Python

---

## Demo


https://github.com/user-attachments/assets/8a3b400c-2d04-4e6c-a0db-1237ff334bd4


---

## Getting Started

### Prerequisites

- Docker & Docker Compose
- Python 3.8+
- Git

## Authors
- Tran Truc Quynh
- Nguyen Minh Huong
- Nguyen Tuan Trong
- Dang Thi Phuong Thao
