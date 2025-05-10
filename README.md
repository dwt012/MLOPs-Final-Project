# Diabetes Prediction MLOps Pipeline

This repository contains an end-to-end MLOps pipeline for diabetes prediction using a modern data architecture that includes batch processing, real-time streaming, data validation, and model deployment.
## Introduction
### Architecture Overview

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

### Workflow
1. **Installation**
   Run this for this project environment settings 
   ```
   pip install -r requirements.txt
    ```
2. **Streaming process**  
   **Description**
   User will enter data through UI, these streaming data will be stored in a table in PostgreSQL
   Then Debezium, which is connector forPostgreSQL will scan the table to check whether the database has new data. The detected new data will be pushed to defined topic in Kafka
   
   **Detail guideline**
   - Create a connector between PostgreSQL and Debezium
    ```
   ./run.sh register_connector ../configs/postgresql-cdc.json
    ```
   - Create a new table on PostgreSQL
   ```
   python create_table.py
    ```
   - Insert data to the table
   ```
     python insert_table.py
   ```
  Access control center Kafka to check the result, topics **diabetes_cdc.public.diabetes_new** is defined topic for Debezium to detect change in PostgreSQL 
     <img width="1440" alt="Ảnh màn hình 2025-05-09 lúc 22 28 04" src="https://github.com/user-attachments/assets/5919d0b1-23bd-4af4-ac99-5cde26f5e211" />

   - To handle streaming datasource, we use Pyflink
    ```
     python datastream_api.py
    ```
   -  This script will check necessary keys in messages as well as filter redundant keys and merge data for later use. Processed samples will be stored back to Kafka in the defnied sink **diabetes_out.public.sink_diabetes**
   -  Message from sink will be fed into diabetes service to get predictions. From then, new data is created and fed into Serving table as well as return prediction on UI for user
3. **Batch Process**  
   Great Expectations validates the processed data before updating the serving table.

---


## Demo


https://github.com/user-attachments/assets/8a3b400c-2d04-4e6c-a0db-1237ff334bd4


---

## Authors
- Tran Truc Quynh
- Nguyen Minh Huong
- Nguyen Tuan Trong
- Dang Thi Phuong Thao
## Reference
[Feature Store Repository by dunghoang369](https://github.com/dunghoang369/feature-store)

