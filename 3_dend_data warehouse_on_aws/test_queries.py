import configparser
import psycopg2


def test_query(cur, conn, tables):
    """
    Run a test query on the data for a
    list of given table names. The test
    query will select all columns and
    limit the result to five rows.
    
    Keyword arguments:
    cur    -- cursor
    conn   -- connection to data base
    tables -- list with table names to run queries 
    """
    for table in tables:
        try:
            cur.execute("SELECT * FROM {} LIMIT 5;".format(table))
            conn.commit()
            print(cur.fetchall())
        except Exception as e:
            print(e)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    tables = ['songplays', 'users', 'songs', 'artists', 'time']

    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
    
        test_query(cur, conn, tables)

        conn.close()

    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
