import pandas as pd 
import os
from sqlalchemy import create_engine

engine = create_engine(r"mysql+pymysql://root:root@localhost:3306/vendor_performance_db")


def load_raw_data():
    for file in os.listdir("dataset"):
        if file.endswith(".csv"):
            df=pd.read_csv("dataset/"+file)
            print(df.shape)
            ingest_db(df, file[:-4], engine)

def ingest_db(df, table_name, engine):
    df.to_sql(table_name, con=engine, if_exists='replace', index=False, chunksize=5000)


if __name__=='__main':
    def load_raw_data()