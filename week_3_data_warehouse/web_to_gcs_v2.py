import io
import os
import requests
from pathlib import Path
import pandas as pd
import pyarrow
import pathlib
from google.cloud import storage

os.environ.setdefault("GCLOUD_PROJECT", "data-engineering-zoomcamp-23")
"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
init_url = 'https://nyc-tlc.s3.amazonaws.com/trip+data/'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "ny-trip-data")

schema_dict = {
                    'VendorID': 'Int64',
                    'passenger_count': 'Int64',
                    'trip_distance': 'float64',
                    'RatecodeID': 'Int64',
                    'store_and_fwd_flag': 'object',
                    'PULocationID': 'Int64',
                    'DOLocationID': 'Int64',
                    'payment_type': 'Int64',
                    'fare_amount': 'float64',
                    'extra': 'float64',
                    'mta_tax': 'float64',
                    'tip_amount': 'float64',
                    'tolls_amount': 'float64',
                    'improvement_surcharge': 'float64',
                    'total_amount': 'float64',
                    'congestion_surcharge': 'float64',
                    }

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def clean_type(df: pd.DataFrame, service:str) -> pd.DataFrame:
    return df.astype(schema_dict)


def clean(df: pd.DataFrame, service) -> pd.DataFrame:
    """Fix dtype issues"""
    if service == "yellow":
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    elif service == "green":
        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    #print(df.head(2))
    #print(f"columns: {df.dtypes}")
    #print(f"rows: {len(df)}")
    return df

def write_local(df: pd.DataFrame, filename: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(filename)
    df.to_parquet(path, compression="gzip")
    return path

def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df


def web_to_gcs(year, service):
    for i in range(12):
        # sets the month part of the file_name string
        month = '0' + str(i + 1)
        month = month[-2:]

        # csv file_name
        file_name = service + '_tripdata_' + year + '-' + month + '.csv'
        dataset_file = f"{service}_tripdata_{year}-{month}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service}/{dataset_file}.csv.gz"
        print("Processing : ", dataset_url)
        # download it using requests via a pandas df
        # request_url = init_url + file_name
        # r = requests.get(dataset_url)
        # pd.DataFrame(io.StringIO(r.text)).to_csv(file_name)
        print(f"Local: {file_name}")
        # df = fetch(dataset_url)

        # read it back into a parquet file
        # df = pd.read_csv(file_name, encoding='latin1')
        # df = clean(df)
        file_name = file_name.replace('.csv', '.parquet')

        df = fetch(dataset_url)

        df_clean = clean(df, service)
        df_clean = clean_type(df_clean, service)
        write_local(df_clean, file_name)
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        '''
        file = pathlib.Path(file_name)
        if file.exists():
            print("File exist")
        else:
            print("File not exist")
            #df.to_parquet(file_name)
            print(f"Parquet: {file_name}")
            # upload it to gcs
            upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
            print(f"GCS: {service}/{file_name}")
        '''


web_to_gcs('2020', 'yellow')
#web_to_gcs('2020', 'yellow')  # web_to_gcs('2019', 'yellow')
# web_to_gcs('2020', 'yellow')

#df = pd.read_parquet("data/green/green_tripdata_2019-04.parquet")
#print(df.dtypes)