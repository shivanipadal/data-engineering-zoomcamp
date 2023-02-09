from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint


@task()
def fetch(dataset_file: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception
    print(dataset_file)
    df = pd.read_csv(dataset_file,compression='gzip')
    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data/fhv/{dataset_file}")
    df.to_csv(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    dataset_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/'
    for i in range(1,13):
        i = f'{i:02d}'
        dataset_file = f"{dataset_url}fhv_tripdata_2019-{i}.csv.gz"
        print(dataset_file)
        file1=f'fhv_tripdata_2019-{i}.csv.gz'

        df = fetch(dataset_file)
        path = write_local(df,  file1)
        write_gcs(path)


if __name__ == "__main__":
    etl_web_to_gcs()
