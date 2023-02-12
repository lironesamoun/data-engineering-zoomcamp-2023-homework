import os
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
from google.cloud import storage
from omegaconf import OmegaConf

root_dir = Path(__file__).parent.parent
config_file = root_dir.joinpath("app.yml")
cfg = OmegaConf.load(config_file)

BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc-trip-data")


def fetch_csv_from(url: str) -> pd.DataFrame:
    print(f"Now fetching: {url}")
    return pd.read_csv(url, engine='pyarrow')


def fix_datatypes_for(df: pd.DataFrame) -> pd.DataFrame:
    return df.astype({'PUlocationID': 'Int64', 'DOlocationID': 'Int64', 'SR_Flag': 'Int64'})


def to_parquet(df: pd.DataFrame, filename: str):
    return df.to_parquet(path=filename, index=False)


def ingest_csv_to_gcs():
    try:
        print("Fetching URL Datasets from .yml")
        datasets = cfg.datasets
        if datasets.fhv:
            for endpoint in datasets.fhv:
                url = urlparse(endpoint)
                filename = str(os.path.basename(url.path).split(".")[0]) + ".parquet"
                filename_path = "data/" + filename
                print("Processing ", filename)
                if not os.path.isfile(filename_path):
                    raw_df = fetch_csv_from(url=endpoint)
                    cleansed_df = fix_datatypes_for(df=raw_df)
                    to_parquet(cleansed_df, filename_path)
                    upload_to_gcs(BUCKET, f"{filename}", filename_path)
                else:
                    upload_to_gcs(BUCKET, f"{filename}", filename_path)

    except Exception as ex:
        print(ex)
        exit(-1)


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    print(f"Uploading {object_name} to GCP - Bucket {bucket}")
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


ingest_csv_to_gcs()
