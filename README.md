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

## Getting Started

Follow these steps to clone the repository and set up the environment.

> **Recommended OS**: macOS or Linux for best compatibility with Docker and Airflow.

### 1. Clone the Repository

```bash
git clone https://github.com/dwt012/MLOPs-Final-Project.git
cd MLOPs-Final-Project
```

---
### 2. Install dependencies:

```bash
pip install -r requirements.txt
```
### Notes

* Ensure Docker is installed and the daemon is running.
* Use a Python version officially supported by your Airflow version.

---
### Workflow
1. **Installation**
   Run this for the project environment settings 
   ```
   pip install -r requirements.txt
    ```
2. **Streaming process**  
   **Description**
   User will enter data through UI, this streaming data will be stored in a table in PostgreSQL
   Then Debezium, which is a connector for PostgreSQL, will scan the table to check whether the database has new data. The detected new data will be pushed to defined topic in Kafka
   
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
   After running this script, we can access Kafka at port 9021:
 
 <img width="1440" alt="Ảnh màn hình 2025-05-11 lúc 01 12 01" src="https://github.com/user-attachments/assets/6e1c149d-5e10-44d2-b658-cd075704c3dc" />

   Topics **diabetes_cdc.public.diabetes_new** is defined topic for Debezium to detect change in PostgreSQL 
     <img width="1440" alt="Ảnh màn hình 2025-05-09 lúc 22 28 04" src="https://github.com/user-attachments/assets/5919d0b1-23bd-4af4-ac99-5cde26f5e211" />
   Observe streaming messages in Message
   <img width="1440" alt="Ảnh màn hình 2025-05-11 lúc 01 21 14" src="https://github.com/user-attachments/assets/d749ff1a-fb85-450f-8d32-eb0a252361a1" />

   - To handle streaming data source, we use Pyflink
    ```
     python datastream_api.py
    ```
   This script will check necessary keys in messages as well as filter redundant keys and merge data for later use. Processed samples will be stored back to Kafka in the defnied sink **diabetes_out.public.sink_diabetes**
   Message from sink will be fed into diabetes service to get predictions. From then, new data is created and fed into Serving table as well as return prediction on UI for user
     <img width="1440" alt="Ảnh màn hình 2025-05-11 lúc 01 20 59" src="https://github.com/user-attachments/assets/f7abb724-4635-43bb-b901-2ccd4ca77a97" />
4. **Batch Process**
   **Guideline**
   - Pick up the latest data of the Diabetes dataset, rewrite the format into deltalake and export it into MinIO object storage as a set of unchangeable raw files.
![image](https://github.com/user-attachments/assets/f902252a-9481-4948-a2ab-0dfa8edc13ba)

   - Then a Spark application will read these raw parquet files directly from MinIO to perform cleansing (e.g. filling null values, filtering the outliers), normalization or encoding the data.
   - After the Spark job is done,  Spark writes the transformed dataset into a staging table in the Postgres data warehouse, this table will hold the full batch of processed records.
   - To validate the data-quality, Great Expectations will run a suite of statements against that table (e.g. distribution test, expect non-null,...). Any validation failures can automatically trigger alerts or stop downstream processes.
   ![image](https://github.com/user-attachments/assets/511ce573-c3a7-47f3-ae54-62bda862a80d)
   - When the batch passes all quality checks, the data is promoted from the staging table into the serving table
   - Finally, an Airflow DAG combines it all together: start the Spark job, executes Great Expectations, sawps in the new serving table, then run a task to retrain the model on this new data.
      
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

