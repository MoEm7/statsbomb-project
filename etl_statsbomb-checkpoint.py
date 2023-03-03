import configparser
import psycopg2
from sql_queries_statsbomb import copy_table_queries 


def load_tables(cur, conn):
    '''
    This function excutes the queries in the copy_table_queries list from sql_queries python file.
    
    This list contains 5 queries mainly:  [staging_events_copy,staging_matches_copy,match_copy,player_copy, team_copy].
    
    These queries copy the all files in s3 buckets 
    '''
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

"""
def insert_tables(cur, conn):
    '''
    This function excutes the queries in the insert_table_queries list from sql_queries python file.
    
    This list contains the Fact and Dimensions insert queries :
    [insert_table_queries]
    
    These queries will fill our star schema with data .
 .
    '''
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
"""

def main():
    '''
    This function is the starting point of the etl.py file .
    
    It creates a config object to parse the configurations of dl.cfg file.
    
    And then connects to the host using the connection string.
    
    Then it calls the other methods in the file [load_tables and  insert_tables]
    '''    
    
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_tables(cur, conn)
    #insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()