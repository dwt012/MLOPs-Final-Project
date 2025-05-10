import json
import os

import pandas as pd
import requests
from confluent_kafka import Consumer, KafkaException


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "mygroup",
            "auto.offset.reset": "latest",
        }
    )

    headers = {
        "accept": "application/json",
        "Content-Type": "application/json",
    }

    consumer.subscribe(["diabetes_out.public.sink_diabetes"])

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            # Parse message
            value = json.loads(msg.value().decode("utf-8"))["data"]
            print(f"Received message: {value}")

            # Send to prediction API
            response = requests.post(
                "http://localhost:4001/predict", headers=headers, json=value
            )

            if response.status_code != 200:
                print("Failed to get prediction!")
                continue

            result = response.json()["result"]  # "Diabetes" or "Normal"
            print("API response:", result)

            # Add Outcome (1 or 0) to record
            dic = {"Diabetes": 1, "Normal": 0}
            value["Outcome"] = dic[result]

            # Save to CSV
            csv_path = "data/diabetes-kafka/diabetes_new.csv"
            new_df = pd.DataFrame([value])
            new_df.columns = [
                "Pregnancies", "Glucose", "BloodPressure", "SkinThickness",
                "Insulin", "BMI", "DiabetesPedigreeFunction", "Age", "Outcome"
            ]

            if os.path.exists(csv_path):
                df = pd.read_csv(csv_path)
                pd.concat([df, new_df]).to_csv(csv_path, index=False)
                print("Appended to existing CSV.")
            else:
                new_df.to_csv(csv_path, index=False)
                print("Created new CSV file.")

            # Return the prediction result as string
            return result

    except KeyboardInterrupt:
        print("Aborted by user!")

    finally:
        consumer.close()


if __name__ == "__main__":
    prediction = main()
    print("Returned prediction:", prediction)  # "Diabetes" or "Normal"
