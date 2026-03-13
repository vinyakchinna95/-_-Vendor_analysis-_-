import pandas as pd 
import os
from sqlalchemy import create_engine
import logging
import time

# Create directories if missing
os.makedirs('logs', exist_ok=True)
os.makedirs('data', exist_ok=True)  # Optional: for testing

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

def load_raw_data():
    start = time.time()
    csv_files = [f for f in os.listdir('data') if f.endswith('.csv')]
    if not csv_files:
        logging.warning('No CSV files found in data directory')
        return
    for file in csv_files:
        df = pd.read_csv('data/' + file)
        logging.info(f'Ingesting {file} into db')
        ingest_db(df, file[:-4], engine)
    end = time.time()
    total_time_taken = (end - start) / 60
    logging.info('---------------Ingestion complete---------------')
    logging.info(f'\\nTotal time taken: {total_time_taken} minutes')

if __name__ == '__main__':
    load_raw_data()
