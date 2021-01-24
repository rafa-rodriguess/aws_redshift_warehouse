import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """   
    - DROP all the tables, if exists, in drop_table_queries
    """
    print("- Dropping tables... ", end='', flush=True)
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()    
    print("Done!")
    
def create_tables(cur, conn):
    """   
    - Create all the tables, if not exists, in create_table_queries
    """
    print("- Creating Tables... ", end='', flush=True)
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
    print("Done!")

def main():
    """   
    - Gets the config info from dwh.cfg
    
    - Establishes connection with the redshift cluster
    
    - DROP the tables and create them.
    
    - Finally, closes the connection. 
    """
    print("- Reading config file... ", end='')
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print("Done!")
    
    print("- Establishing connection with Database: {}, user: {} on host: {} and port: {}... ".format(config.get("CLUSTER","DB_NAME"), config.get("CLUSTER","DB_USER"), config.get("CLUSTER","HOST"), config.get("CLUSTER","DB_PORT")), end='', flush=True)
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("Done!")
    
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    print("- Closing Connection... ", end='')
    conn.close()
    print("Done!")

if __name__ == "__main__":
    main()