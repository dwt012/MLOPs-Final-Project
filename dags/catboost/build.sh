#!/bin/bash
docker build -t nmh/airflow-cb-stage:0.0.1 .
docker push nmh/airflow-cb-stage:0.0.1