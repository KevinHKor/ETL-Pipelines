import psycopg2
from datetime import date, timedelta
from config.config import Config as cfg

# Connect to the PostgreSQL database
connection = psycopg2.connect(
    host=cfg.host,
    port=cfg.port,
    database=cfg.database,
    user=cfg.user,
    password=cfg.password
)

def transform_data():
    '''
    Calls the stored procedure on PostgreSQL db to transform the data.
    '''

    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM {table}".format(table='final.crime_logs'))
    # Fetch the result of the query
    count = cursor.fetchone()[0]
    if count==0:
        processed_date = ''
    else:
        processed_date = str(date.today() - timedelta(days=3))

    try:
        cursor = connection.cursor()
        if processed_date:
            cursor.execute("CALL final.sp_transform_crime_logs('{date}'::date)".format(date=processed_date))
        else:
            cursor.execute("CALL final.sp_transform_crime_logs(NULL)")  # Pass NULL when the parameter is empty
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        connection.rollback()
    finally:
        cursor.close()