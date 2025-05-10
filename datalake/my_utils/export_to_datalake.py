import os
from glob import glob

from helpers import load_cfg
from minio import Minio

CFG_FILE = r"C:\Users\Dell\OneDrive - National Economics University\code\MLOps\datalake\my_utils\config.yaml"


def main():
    cfg = load_cfg(CFG_FILE)
    data_cfg = cfg["datastore"]
    datalake_cfg = cfg["datalake"]

    endpoint = datalake_cfg["endpoint"].strip()

    client = Minio(
        endpoint=endpoint,  
        access_key=datalake_cfg["access_key"],
        secret_key=datalake_cfg["secret_key"],
        secure=False,  
    )

    # Create bucket if not exist.
    found = client.bucket_exists(bucket_name=datalake_cfg["bucket_name_diabetes"])
    if not found:
        client.make_bucket(bucket_name=datalake_cfg["bucket_name_diabetes"])
    else:
        print(
            f'Bucket {datalake_cfg["bucket_name_diabetes"]} already exists, skip creating!'
        )

    # Upload files.
    all_parquet_files = glob(
        os.path.join(data_cfg["diabetes_deltalake_path"], "**/*.parquet"),
        recursive=True,
    )

    all_json_files = glob(
        os.path.join(data_cfg["diabetes_deltalake_path"], "**/*.json"), recursive=True
    )
    print(all_parquet_files)
    print(all_json_files)

    for fp in all_parquet_files:
        object_name = os.path.join(
            datalake_cfg["folder_name_diabetes"], os.path.basename(fp)
        )
        print(f"Uploading {fp} to {object_name}")
        client.fput_object(
            bucket_name=datalake_cfg["bucket_name_diabetes"],
            object_name=object_name,
            file_path=fp,
        )

    for fp in all_json_files:
        object_name = os.path.join(
            datalake_cfg["folder_name_diabetes"], os.path.basename(fp)
        )
        print(f"Uploading {fp} to {object_name}")
        client.fput_object(
            bucket_name=datalake_cfg["bucket_name_diabetes"],
            object_name=object_name,
            file_path=fp,
        )


if __name__ == "__main__":
    main()