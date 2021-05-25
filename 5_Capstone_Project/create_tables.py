import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    - Creates and connects to the sparkifydb
    @return: cursor and connection to sparkifydb
    """
    
    # connect to default database
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
        print("connected to default db")
    except psycopg2.Error as e: 
        print("Error: Could not make connection to the Postgres database")
        print(e)
        
    try:
        conn.set_session(autocommit=True)
        cur = conn.cursor()
        
    except psycopg2.Error as e: 
        print("Error: Could not get curser to the Database")
        print(e)
    
    # create sparkify database with UTF8 encoding
    try:
        sql = "DROP DATABASE IF EXISTS sparkifydb"
        cur.execute(sql)
        sql = "CREATE DATABASE sparkifydb"# ENCODING 'UTF8' TEMPLATE template0"
        cur.execute(sql)
        print("created sparkifydb db")
        
    except psycopg2.Error as e:
        print(conn.closed)
        print("Error: Could not get create the Database")
        print(e)
        
    conn.close()    
    
    # connect to sparkify database
    
    # Establish connection to sparkify database
    try:
        conn = psycopg2.connect("host=localhost dbname=sparkifydb user=student password=student")
        cur = conn.cursor()
        print("connection established")
    except psycopg2.Error as e:
        print("Error: Could not connect to SPARKIFY database")
        print(e)  
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    @param cur:
    @param conn:
    """
    try:
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()
    except psycopg2.Error as e:
        print("Error: Could not drop the tables") 
        print(e) 


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list.
    @param cur:
    @param conn:
    """
    try:
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()
    except psycopg2.Error as e:
        print("Error: Could not create the table: "+ table_name)
        print(e)

def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    #Invoke all the functions
    cur, conn = create_database()
    print("Drop existing tables")
    drop_tables(cur,conn)
    print("Create tables")
    create_tables(cur, conn)
    
    # close the connection to sparkifydb
    
    conn.close()
    print("closing the connection")


if __name__ == "__main__":
    main()