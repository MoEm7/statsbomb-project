import configparser
import psycopg2
from sql_queries_statsbomb import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    This function excutes the queries in the drop_table_queries list from sql_queries python file.
    
    This list contain queries that drop all our schema tables one by one if they exist.
    This helps to recreate the tables from scratch each time we run the file.
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    This function excutes the queries in the create_table_queries list from sql_queries python file.
    
    This list contain queries that create all our schema tables one by one if they don't exist.
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    '''
    This function is the starting point of the create_tables.py file .
    
    It creates a config object to parse the configurations of dwh.cfg file.
    
    And then connects to the host using the connection string.
    
    Then it calls the other methods in the file [drop_tables and  create_tables]
    '''    

    config = configparser.ConfigParser()
    config.read('dl.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()