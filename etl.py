import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        print("finish load one staging table.")
        conn.commit()
        print("finish upload one staging table to s3.")


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        print("finish load one objective table.")
        conn.commit()
        print("finish upload one objective table to redshift.")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    
    cur = conn.cursor()
    
    print("Loading source data from S3 to staging tables.")
    
    load_staging_tables(cur, conn)
    
    print("Transform staging data to destination tables.")
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()