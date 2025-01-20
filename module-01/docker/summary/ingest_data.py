# https://stackoverflow.com/questions/40216311/reading-in-environment-variables-from-an-environment-file
import os
from time import time
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv


def main():
    load_dotenv()
    # Fetch the environment variables
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    host = os.getenv('POSTGRES_HOST')  # use service name as host
    port = os.getenv('POSTGRES_PORT')
    db = os.getenv('POSTGRES_DB')
    table_name = os.getenv('POSTGRES_TABLE_NAME')
    url = os.getenv('CSV_URL')  # Set this as an environment variable or pass a default URL
    csv_name = 'yellow_tripdata_2021-01.csv'

    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    # KT I commented this part out - I just downloaded the csv locally, couldn't get this part to work
    # if url.endswith('.csv.gz'):
    #     csv_name = 'output.csv.gz'
    # else:
    #     csv_name = 'output.csv'
    #
    # os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}')

    # df_iter = pd.read_csv(csv_name)
    # df_iter.to_sql(name=csv_name, con=engine, if_exists='append')

    df_zones = pd.read_csv('taxi_zone_lookup.csv')
    df_zones.to_sql(name='zones', con=engine, if_exists='replace')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        try:
            t_start = time()
            df = next(df_iter)
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.to_sql(name=table_name, con=engine, if_exists='append')
            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break



if __name__ == '__main__':
    main()
