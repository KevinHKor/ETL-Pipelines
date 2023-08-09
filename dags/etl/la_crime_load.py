import pandas as pd
# import snowflake.connector
import psycopg2
from config.config import Config as cfg

class load:
    def load_final():
        '''
        Loads sformed dtranata from final staging tables to a dataframe.
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
        
    def load_to_dw(df):
        '''
        Loading to Snowflake datawarehouse.
        '''
        pass
        # snowflake_connection = snowflake.connector.connect(
        #     user=cfg.snowflake_username,
        #     password=cfg.snowflake_password,
        #     account=cfg.snowflake_account
        # )

        # snowflake_cursor = snowflake_connection.cursor()

        # database_name = 'etl'

        # snowflake_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        # snowflake_cursor.execute(f"USE DATABASE {database_name}")

        # table_name = 'crime_data'
