import argparse
import os
from glob import glob

import dotenv
import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.feature import MinMaxScaler, VectorAssembler
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from pyspark.ml.functions import vector_to_array   # Spark ≥ 3.1


dotenv.load_dotenv()

os.environ["JAVA_HOME"] = "C:\\Program Files\\OpenLogic\\jdk-11.0.27.6-hotspot"
os.environ["SPARK_HOME"] = r"C:\Users\Dell\Downloads\spark-3.5.5-bin-hadoop3\spark-3.5.5-bin-hadoop3"
os.environ["HADOOP_HOME"] = r"C:\hadoop-3.3.3"
os.environ["PATH"] += os.pathsep + r"C:\hadoop-3.3.1\bin"

if __name__ == "__main__":
    # The entrypoint to access all functions of Spark
    spark = (
    SparkSession.builder.master("local[*]")
        .appName("Python Spark read parquet example")
        # --- tell Hadoop-AWS how to reach MinIO ---
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")   
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
)

    parquet_files = spark.read.parquet("s3a://mlops.data/diabetes.parquet")
    unlist = udf(lambda x: round(float(x), 3) if x is not None else None, DoubleType())
    normalized_features = [
        "Pregnancies",
        "BloodPressure",
        "SkinThickness",
        "Insulin",
        "Age",
    ]
    for i, parquet_file in enumerate(parquet_files):
        df = spark.read.parquet(parquet_file)
    # df = spark.read.parquet("s3a://mlops.data/diabetes.parquet") - 1 loop
    # Check null value of dataframe
    df.printSchema()
    # for feature in normalized_features:
    #     # Convert column to vector type
    #     vector_assembler = VectorAssembler(
    #         inputCols=[feature], outputCol=f"{feature}_vect"
    #     )

    #     # Initialize min-max scaler
    #     scaler = MinMaxScaler(
    #         inputCol=f"{feature}_vect", outputCol=f"{feature}_normed"
    #     )

    #     # Add 2 processes to pipeline to transform dataframe
    #     pipeline = Pipeline(stages=[vector_assembler, scaler])
    #     df = (
    #         pipeline.fit(df)
    #         .transform(df)
    #         .withColumn(f"{feature}_normed", unlist(f"{feature}_normed"))
    #         .drop(f"{feature}_vect", feature)
    #     )

    for feature in normalized_features:
        assembler = VectorAssembler(inputCols=[feature],
                                    outputCol=f"{feature}_vect")
        scaler = MinMaxScaler(inputCol=f"{feature}_vect",
                            outputCol=f"{feature}_normed_vec")

        df = (Pipeline(stages=[assembler, scaler]).fit(df).transform(df)
                # vector_to_array converts DenseVector ➜ Spark array
                .withColumn(f"{feature}_normed",
                            F.round(vector_to_array(f"{feature}_normed_vec")[0], 3))
                .drop(f"{feature}_vect", f"{feature}_normed_vec", feature))

        df.show()

    df.printSchema()
    df_pandas = pd.DataFrame(df.toPandas())
    print("Saving DataFrame to:", "C:\\Users\\Dell\\OneDrive - National Economics University\\code\\MLOps\\final_data\\diabetes_1.csv")
    df_pandas.to_csv("C:\\Users\\Dell\\OneDrive - National Economics University\\code\\MLOps\\final_data\\diabetes_1.csv")
