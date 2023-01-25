#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_names = params.table_names
    urls = params.urls
    
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file


    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    for table_name,url in list(zip(table_names,urls)):
        print(f'Table name is {table_name}, url is {url}')
        
        if url.endswith('.csv.gz'):
            csv_name = table_name + '.csv.gz'
        else:
            csv_name = table_name + '.csv'
        
        os.system(f"wget {url} -O {csv_name}")
        
        if (table_name.endswith('trips')) :
             df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
        else:
             df_iter=pd.read_csv(csv_name, iterator=True, chunksize=500)
        
        df = next(df_iter)
        
        if (table_name == 'yellow_taxi_trips'):
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        elif (table_name == 'green_taxi_trips'):
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        
        df.to_sql(name=table_name, con=engine, if_exists='append')
        
        
        while True: 
        
            try:
                t_start = time()
                
                df = next(df_iter)
                if (table_name == 'yellow_taxi_trips') :
                    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
                    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
                elif (table_name == 'green_taxi_trips') :
                    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
                    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
                
                df.to_sql(name=table_name, con=engine, if_exists='append')
        
                t_end = time()
        
                print('inserted another chunk, took %.3f second' % (t_end - t_start))
        
            except StopIteration:
                print("Finished ingesting data into the postgres database")
                break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_names', required=True, help='name of the table where we will write the results to',nargs=3)
    parser.add_argument('--urls', required=True, help='url of the csv file',nargs=3)

    args = parser.parse_args()

    main(args)
