'''
    Author: Varun Vasudev
    Create and drop tables from the list present in sql_queries.py script
'''
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


# create the SPARKIFY database if it does not exists

def create_database():
    # Create a connection to the database
    try: 
        conn = psycopg2.connect("host=localhost dbname=studentdb user=student password=student")
        #print("connection success")

    except psycopg2.Error as e: 
        print("Error: Could not make connection to the Postgres database")
        print(e)

    # Use the connection to get a cursor that can be used to execute queries.
    try: 
        cur = conn.cursor()
        conn.set_session(autocommit=True)
                
    except psycopg2.Error as e: 
        print("Error: Could not get curser to the Database")
        print(e)

    try:
        sql = "DROP DATABASE IF EXISTS sparkifydb"
        cur.execute(sql)
        sql = "CREATE DATABASE sparkifydb"
        cur.execute(sql)
        
    except psycopg2.Error as e: 
        print("Error: Could not get create the Database")
        print(e)

    # Close the connection to default database

    conn.close()

    # Establish connection to sparkify database
    try:
        conn = psycopg2.connect("host=localhost dbname=sparkifydb user=student password=student")
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not connect to SPARKIFY database")    
    return cur, conn

def drop_tables(cur,conn):

     #Drop the tables users_drop_table,artists_drop_table,songs_drop_table,time_drop_table,songplays_drop_table if exists before creating them in sparkifydb
    try:
        for table_name in drop_table_queries:
            cur.execute(table_name)
            conn.commit()
    except psycopg2.Error as e:
        print("Error: Could not drop the tables")  
        
def create_tables(cur,conn):
    #Create the tables users_create_table,artists_create_table,songs_create_table,time_create_table,songplays_create_table in sparkifydb
    try:
        for table_name in create_table_queries:
            cur.execute(table_name)
            conn.commit()
            #print("created table: "+ table_name)
    except psycopg2.Error as e:
        print("Error: Could not create the table: "+ table_name)

if __name__ == '__main__':
    #Invoke all the functions
    cur, conn = create_database()
    drop_tables(cur,conn)
    create_tables(cur, conn)
    
    # close the connection to sparkifydb

    conn.close()