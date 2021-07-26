import json
import psycopg2

conn =  mysql.connector.connect(user='root', password='root',
                          host='localhost', database='northwind') 

def conn(query, args=(), one=False):
    cur = conn().cursor()
    cur.execute(query, args)
    r = [dict((cur.description[i][0], value) \
               for i, value in enumerate(row)) for row in cur.fetchall()]
    cur.connection.close()
    return (r[0] if r else None) if one else r

my_query = query_db("SELECT * FROM customers %s", (3,))

json_output = json.dumps(my_query)
