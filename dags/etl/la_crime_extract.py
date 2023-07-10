import requests
import pandas as pd
import boto3
from psycopg2 import connect, extras
from datetime import date, timedelta
from config.config import Config as cfg

# Connect to the AWS RDS - PostgreSQL database.
connection = connect(
    host=cfg.host,
    port=cfg.port,
    database=cfg.database,
    user=cfg.user,
    password=cfg.password
)

def process_open_data():
    '''
    If the table is empty, we will run a full load. Otherwise, it will be a batch update (day of most recent records).
    '''
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM {table}".format(table='public.crime_logs'))
    # Fetch the result of the query
    count = cursor.fetchone()[0]

    if count==0:
        open_data_staging_fl()
    else:
        open_data_staging_batch()

def open_data_staging_fl():
    '''
    Returns Los Angeles Crime Data (Open Data) - Crime Data from 2020 to Present.
    Because the data is so large (+700,000 rows), the most efficient way to load it into the staging table is to do a copy from CSV.
    '''
    api_url = 'https://data.lacity.org/resource/2nrs-mtv8.json?$limit=1000000'
    data = requests.get(api_url).json()

    # Converting json data to pandas dataframe
    data_df = pd.DataFrame.from_dict(data)

    # Dropping the following uncessary columns
    data_df = data_df.drop(columns=['date_occ', 'part_1_2', 'cross_street','crm_cd_3','crm_cd_4'], axis=1, errors='ignore')

    # Load into CSV. Converting all NaN values of a dataframe to null.
    data_df.to_csv(cfg.crime_log_fl_csv, na_rep='null', index=False, header=False) #Name the .csv file reference in line 29 here

    #Remove additional spaces between words in address field.
    data_df['location'] = data_df['location'].str.replace('\s+',' ',regex=True)

    csv_file_name = cfg.crime_log_fl_csv
    sql = "COPY public.crime_logs FROM STDIN DELIMITER ',' CSV  NULL AS 'null' HEADER"

    try:
        cursor = connection.cursor()
        cursor.copy_expert(sql, open(csv_file_name, "r"))
        connection.commit()
    except (Exception, connect.DatabaseError) as error:
        print("Error: %s" % error)
    finally:
        cursor.close()

def open_data_staging_batch():
    ''' 
    Returns Los Angeles Crime Data (Open Data) - most recent date. There's a 3 day lag between current date and most recent record.
    '''
    processed_date = str(date.today() - timedelta(days=3))
    api_url = 'https://data.lacity.org/resource/2nrs-mtv8.json?date_rptd=' + processed_date
    data = requests.get(api_url).json()

    # Converting json data to pandas dataframe
    data_df = pd.DataFrame.from_dict(data)

    # Dropping the following unnecessary columns
    data_df = data_df.drop(columns=['date_occ', 'part_1_2', 'cross_street', 'crm_cd_3', 'crm_cd_4'], axis=1, errors='ignore')

    # Converting all NaN values of a dataframe to null
    data_df = data_df.where(pd.notnull(data_df), None)

    #Remove additional spaces between words in address field.
    data_df['location'] = data_df['location'].str.replace('\s+',' ',regex=True)

        # Create a list of tuples from the dataframe values
    tuples = [tuple(x) for x in data_df.to_numpy()]

    query = ("INSERT INTO {table}({columns}) VALUES %s".format(table='public.crime_logs',columns=','.join(data_df.columns)))
        
    try:
        cursor = connection.cursor()
        extras.execute_values(cursor, query, tuples)    
        connection.commit()
    except (Exception, connect.DatabaseError) as error:
        print("Error: %s" % error)
        connection.rollback()
    finally:
        cursor.close()

def process_s3_mo_codes():
    '''
    Returns mo_Codes stored in s3 bucket and stores it in a staging table in a PostgreSQL database.
    This will only run if the staging table is empty.
    '''

    # Creating the low level functional client
    client = boto3.client(
        's3',
        aws_access_key_id=cfg.aws_access_key_id,
        aws_secret_access_key=cfg.aws_secret_access_key,
        region_name=cfg.region_name
    )

    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM {table}".format(table='public.mo_codes_lookup'))
    # Fetch the result of the query
    count = cursor.fetchone()[0]
    
    if count==0:
        # Create df for s3 csv
        moCode_obj = client.get_object(Bucket=cfg.bucket_name, Key=cfg.bucket_key)
        moCode_df = pd.read_csv(moCode_obj['Body'])
        moCode_df = moCode_df.rename(columns={'Code':'code_id'})

        # Create a list of tupples from the dataframe values
        tuples = [tuple(x) for x in moCode_df.to_numpy()]
        # Comma-separated dataframe columns
        cols = ','.join(list(moCode_df.columns))
        # SQL quert to execute
        query  = "INSERT INTO %s(%s) VALUES(%%s,%%s)" % ('mo_codes_lookup', cols)

        try:
            cursor.executemany(query, tuples)
            connection.commit()
        except (Exception, connect.DatabaseError) as error:
            print("Error: %s" % error)
            connection.rollback()
        finally:
            cursor.close()