import pandas as pd
import numpy as np
import snowflake.connector
import psycopg2
from config.config import Config as cfg
import datetime

class load:
    def format_value(val):
        if val is not None and not pd.isna(val):
            if isinstance(val, str):
                modified_val = val.replace("\\", "\\\\").replace("'", "''")
                return f"'{modified_val}'"
            elif isinstance(val, datetime.date):  # Handle pandas DateOffset (date) objects
                return "'" + val.strftime('%Y-%m-%d') + "'"
            else:
                return str(val)
        else:
            return 'NULL'
    
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
        
        try:
            cursor.execute(query)
        except(Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" %error)
            cursor.close()
            
        tuples_list = cursor.fetchall()
        cursor.close()

        df_to_load = pd.DataFrame(tuples_list)

        return df_to_load
        
    def load_to_dw(df):
        '''
        Loading to Snowflake datawarehouse.
        '''
        snowflake_connection = snowflake.connector.connect(
            user=cfg.snowflake_username,
            password=cfg.snowflake_password,
            account=cfg.snowflake_account
        )

        snowflake_cursor = snowflake_connection.cursor()

        snowflake_cursor.execute("USE DATABASE ETL")

        # Assuming CITIBIKE is the schema and TRIPS is the table in Snowflake
        schema = cfg.snowflake_etl_crime_schema
        table = cfg.snowflake_etl_crime_table

        try:
            # Assuming 'dataframe' is the pandas DataFrame containing data from PostgreSQL
            for _, row in df.iterrows():
                values = ','.join([load.format_value(val) for val in row])     
                insert_query = f"INSERT INTO {schema}.{table} VALUES ({values})"
                snowflake_cursor.execute(insert_query)
        except(Exception, psycopg2.DatabaseError) as error:
            print(insert_query)
            print('Error: %s '% error)
            snowflake_cursor.close()
        finally:
            snowflake_connection.commit()
            snowflake_cursor.close()
            snowflake_connection.close()