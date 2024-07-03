import argparse
import os
from typing import Any, Optional
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def read_data(file_path: str) -> Optional[pd.DataFrame]:
    if file_path.endswith('.csv').lower():
        df =  pd.read_csv(file_path, engine='pyarrow')
    elif file_path.endswith('.parquet').lower():
        df = pd.read_parquet(file_path, engine="pyarrow")
    else:
        return ValueError("Invalid file type. Please use .parquet or .csv only.")
    
def transform_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[:, 'tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df.loc[:, 'tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    return df

def load_to_sql(file_path: str, user: str, password: str, host: str, 
                port: str, db: str, table_name: str) -> None:
    
    df = read_data(file_path)
    df = transform_columns(df)

    DB_URI = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(DB_URI)
    engine.connect()
    
    chunk = df[0:10000]
    chunk.head(n=0).to_sql(name=table_name, con=engine, if_exist="replace")
    chunk.to_sql(name=table_name, con=engine, if_exists="append")

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Ingest .csv data to Postgres')

    parser.add_argument('user', help='username for postgres')
    parser.add_argument('password', help='password for postgres')
    parser.add_argument('host', help='host for postgres')
    parser.add_argument('port', help='port for postgres')
    parser.add_argument('db', help='database name for postgres')
    parser.add_argument('table_name', help='table name where results are written')
    parser.add_argument('url', help='url of csv')
    parser.add_argument('file_type', help='file type being inputted')
    parser.add_argument('file_path', help='path to the file being inputted(parquet or csv)')
    return parser.parse_args()

def main(parameters: argparse.Namespace) -> None:
    args = parse_arguments()
    file_path = args.file_path
    df = read_data(file_path)
    df = transform_columns(df)
    
    load_to_sql(
        df = df,
        file_path = parameters.file_path,
        user = parameters.user,
        password = parameters.password,
        host = parameters.host,
        port = parameters.port,
        db = parameters.db,
        table_name = parameters.table_name
    )


if __name__=="__main__":
    main()