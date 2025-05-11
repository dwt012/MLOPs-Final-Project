#!/bin/bash
docker build -t dawngthao/airflow-cb-stage:0.0.1 .
docker push dawngthao/airflow-cb-stage:0.0.1
