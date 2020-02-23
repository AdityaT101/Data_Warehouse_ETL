import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


#This function is used to drop all the tables if exists in the Redshift
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

        
#This function is used to create all the tables if not exists in the Redshift
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()



def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    #connecting to the database 
    try:
      conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
      print(conn)
    except psycopg2.Error as e:
      print("could not make a connection to the database")
      print(e)
      
  
    try:
    #establishing a cursor
      cur = conn.cursor()
    except psycopg2.Error as e:
      print("could not create a cursor")
      print(e)
    
   
    drop_tables(cur, conn)
    create_tables(cur, conn)
    

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()