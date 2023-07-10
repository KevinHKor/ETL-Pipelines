import pandas as pd
import boto3
import psycopg2
from config.config import Config as cfg

def load():
    '''
    Loads transformed data from final staging tables to a dataframe.
    '''

    # Connect to the PostgreSQL database
    connection = psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        database=cfg.database,
        user=cfg.user,
        password=cfg.password
    )

    query  = "SELECT * FROM final.crime_logs"
    cursor = connection.cursor()
    
    df_to_load = pd.read_sql_query(query,connection)

    load_to_dw(df_to_load)
    
def load_to_dw(df):
    '''
    Loading to Snowflake datawarehouse.
    '''
    pass