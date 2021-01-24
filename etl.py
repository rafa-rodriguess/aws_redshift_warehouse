import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """   
    - load data from S3 bucket into stageArea tables
    """
    print("- Loading tables from S3 to Stage Area. It might take a while... ", end='', flush=True)
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    print("Done!")

def insert_tables(cur, conn):
    """   
    - Insert into the final tables
    """
    print("- Insert Data from Stage Area to Final tables. it might take a while... ", end='', flush=True)
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    print("Done!")

def main():
    """   
    - Gets the config info from dwh.cfg
    
    - Establishes connection with the redshift cluster
    
    - DROP the tables, loads them from S3 Bucket first into stage area and than to the tables
    
    - Finally, closes the connection. 
    """
    print("- Reading config file... ", end='')
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print("Done!")
    
    print("- Establishing connection with Database: {}, user: {} on host: {} and port: {}... ".format(config.get("CLUSTER","DB_NAME"), config.get("CLUSTER","DB_USER"),config.get("CLUSTER","HOST"), config.get("CLUSTER","DB_PORT")),end='' ,flush=True)
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Done!")
    
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    print("- Closing Connection... ", end='')
    conn.close()
    print("Done!")

if __name__ == "__main__":
    main()