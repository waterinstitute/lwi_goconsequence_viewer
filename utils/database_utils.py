import psycopg2
from io import StringIO


def get_db_connection(database, user, password, host, port):
    try:
        con = psycopg2.connect(
            database=database, user=user, password=password, host=host, port=port
        )
    except psycopg2.OperationalError as e:
        print(f"Error connecting to database: {e}")
        return None
    return con


def copy_from_stringio(conn, df, table):
    """
    Function that allows inserting multiple rows thourhg a Pandas dataframe into a
    PostgreSQL table, this function is based on the copy_from() which is the fastest
    method to insert data into a PostgreSQL table.
    Here we are going save the dataframe in memory
    and use copy_from() to copy it to the table in PostgreSQL connection
    params:
    - conn: psycopg2 connection
    - df: Pandas dataframe
    - table: PostgreSQL table name

    """
    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    cursor = conn.cursor()
    try:
        cursor.copy_from(buffer, table, sep=",", null="", columns=df.columns)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("copy_from_stringio() done")
    cursor.close()
