from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(path: Path) -> int:
    """Write DataFrame to BiqQuery"""

    df = pd.read_parquet(path)
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="ny-rides-shivani",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )
 
    return len(df)

@flow(log_prints=True)
def etl_gcs_to_bq(year, month, color):
    """Main ETL flow to load data into Big Query"""

    path = extract_from_gcs(color, year, month)
    #df = transform(path)
    records=write_bq(path)

    return records

@flow(log_prints=True)
def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):
    rec_count = 0
    for month in months:
        records=etl_gcs_to_bq(year, month, color)
        rec_count = rec_count + records

    print("Total no of records processed", rec_count)

if __name__ == "__main__":
    color = "yellow"
    months = [2,3]
    year = 2019
    etl_parent_flow(months, year, color)
