from pathlib import Path 
import pandas as pd 
from prefect import flow, task 
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def fetch(dataset_url):
    df = pd.read_csv(dataset_url)
    return df 

@task()
def write_local(df, color, dataset_file):
    path = Path(f"data/{color}/{dataset_file}.csv.gz")
    gcp_path = Path(f"data/{color}/{dataset_file}.csv.gz")
    df.to_csv(path, compression="gzip")
    return path, gcp_path

@task()
def write_gcs(path, gcp_path):
    gcs_block = GcsBucket.load("zoomcamp-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)

@flow()
def etl_web_to_gcs(year, month, color):
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    path, gcp_path = write_local(df, color, dataset_file)
    write_gcs(path, gcp_path)


@flow()
def etl_parent_flow(months, year, color):
    for month in months:
        etl_web_to_gcs(year, month, color)


if __name__ == "__main__":
    color = "fhv"
    year = 2019
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    etl_parent_flow(months, year, color)


