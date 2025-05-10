import json
import os

from pyflink.common import Configuration, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer, KafkaRecordSerializationSchema, KafkaSink, KafkaSource
)
import time

# ==== Cấu hình JAR path ====
JARS_PATH = "/Users/dangthiphuongthao/Documents/Apps/jars"

def print_features(record):
    """Trích xuất phần 'after' từ Kafka CDC message."""
    record = json.loads(record)
    data = record["payload"]["after"]
    print("print_features: Data extracted from record:", data)  # Add log for feature extraction
    return json.dumps(data)

def filter_features(record):
    """Lọc bớt các cột không cần thiết khỏi record."""
    record = json.loads(record)
    data = {key: val for key, val in record.items() if key not in ["created", "content"]}
    print("filter_features: Filtered data:", data)  # Add log for filtered data
    return json.dumps({"created": record.get("created"), "data": data})

def check_record_keys(record):
    """Kiểm tra bản ghi Kafka có chứa key 'payload' không."""
    record = json.loads(record)
    if "payload" in record:
        print("check_record_keys: Payload found in record.")  # Log if payload found
        return True
    print("check_record_keys: No payload found, keys:", list(record.keys()))  # Log missing payload
    return False

# ==== Gói toàn bộ pipeline vào hàm main ====
def main():
    print("datastream_main: Starting job...")  # Log to indicate job start
    start_time = time.time()  # Track execution start time
    
    config = Configuration()
    config.set_string(
        "pipeline.jars",
        ";".join([
            f"file://{JARS_PATH}/flink-connector-kafka-4.0.0-2.0.jar",
            f"file://{JARS_PATH}/kafka-clients-3.6.0.jar"
        ])
    )

    env = StreamExecutionEnvironment.get_execution_environment(configuration=config)
    print("datastream_main: Flink environment initialized.")  # Log after Flink environment setup
    
    # Kafka Source
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("localhost:9092")
        .set_topics("diabetes_cdc.public.diabetes_new")
        .set_group_id("diabetes-consumer-group")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    print("datastream_main: Kafka Source configured.")  # Log after Kafka source setup
    
    # Kafka Sink
    sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("http://localhost:9092")
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("diabetes_out.public.sink_diabetes")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )
    print("datastream_main: Kafka Sink configured.")  # Log after Kafka sink setup

    # DataStream pipeline
    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
        .filter(check_record_keys)
        .map(print_features, output_type=Types.STRING())
        .map(filter_features, output_type=Types.STRING())
        .sink_to(sink)
    )
    print("datastream_main: Pipeline created and ready to execute.")  # Log before executing job

    # Execute the job
    env.execute("flink_datastream_demo")
    print("datastream_main: Job executed.")  # Log after job execution

    execution_time = time.time() - start_time  # Calculate execution time
    print(f"datastream_main: Job completed in {execution_time} seconds.")  # Log execution time

if __name__ == "__main__":
    main()
