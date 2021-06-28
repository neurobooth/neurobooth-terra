import psycopg2

try:
    connect_str = "dbname='neurobooth' user='neuroboother' host='localhost' " + \
                  "password='neuroboothrocks'"
    # use our connection values to establish a connection
    conn = psycopg2.connect(connect_str)
    cursor = conn.cursor()
    # create a new table with a single column called "name"
    cursor.execute("""CREATE TABLE tutorials (name char(40));""")
    # run a SELECT statement - no data in there, but we can try it
    cursor.execute("""SELECT * from tutorials""")
    conn.commit() # <--- makes sure the change is shown in the database
    rows = cursor.fetchall()
    print(rows)
    cursor.close()
    conn.close()
except Exception as e:
    print("Uh oh, can't connect. Invalid dbname, user or password?")
    print(e)
