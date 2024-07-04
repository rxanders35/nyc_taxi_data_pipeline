import argparse
import os
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

def read_csv(file_path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(file_path)
    except FileNotFoundError:
        raise FileNotFoundError(f".csv file not found:{file_path}")

def read_parquet(file_path: str) -> pd.DataFrame:
    try:
        return pq.read_table(file_path).to_pandas()
    except FileNotFoundError:
        raise FileNotFoundError(f".parquet file not found:{file_path}")
    
def transform_columns(df: pd.DataFrame) -> pd.DataFrame:
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    return df

def load_chunk_to_sql(df: pd.DataFrame, user: str, password: str, host: str, 
                port: str, db: str, table_name: str, chunk_size: int=10000) -> None:
    DB_URI = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    
    try: 
        engine = create_engine(DB_URI)
        with engine.connect() as conn:
            df.head(n=0).to_sql(name=table_name, con=conn, if_exists="replace", index=False)

            for start in range(0, len(df), chunk_size):
                end = start + chunk_size
                chunk = df.iloc[start:end]
                chunk.to_sql(name=table_name, con=engine, if_exists="append", index=False)
    except SQLAlchemyError as e:
        raise SQLAlchemyError(f"Database connection error: {str(e)}")

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Ingest .csv data to Postgres')

    parser.add_argument('user', help='username for postgres')
    parser.add_argument('password', help='password for postgres')
    parser.add_argument('host', help='host for postgres')
    parser.add_argument('port', help='port for postgres')
    parser.add_argument('db', help='database name for postgres')
    parser.add_argument('table_name', help='table name where results are written')
    parser.add_argument('file_path', help='path to the file being inputted(parquet or csv)')
    
    return parser.parse_args()

def main() -> None:
    args = parse_arguments()
    file_path = args.file_path
    file_extension = os.path.splitext(file_path)[1].lower()
    
    if file_extension == '.csv':
        df = read_csv(file_path)
    elif file_extension == '.parquet':
        df = read_parquet(file_path)
    else:
        raise ValueError("Please input a .csv or .parquet file only.")

    df_transformed = transform_columns(df)
    
    load_chunk_to_sql(
        df = df_transformed,
        user = args.user,
        password = args.password,
        host = args.host,
        port = args.port,
        db = args.db,
        table_name = args.table_name
    )

if __name__=="__main__":
    main()