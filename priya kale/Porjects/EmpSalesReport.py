import  mysql.connector
import pandas as pd
import psycopg2
import json

# ----------------------table A ------------------------

conn =  mysql.connector.connect(user='root', password='root',
                          host='localhost', database='northwind')                         
cur = conn.cursor()
cur.execute("SELECT * FROM employees")  
rows = cur.fetchall()
for r in rows:
  alldata =(f"  \t{r[0]},\t{r[1]},   \t{r[2]},    \t{r[3]}, \t{r[4]}, \t{r[5]}, ")
  print (alldata)
print("------------------------------------------------------------------------------------------------------------------")

df = pd.read_sql('SELECT * FROM employees', con=conn)
df.to_json()
print(df)


# ----------------------table B ------------------------


cur = conn.cursor() 
cur.execute("SELECT * FROM orders")                               
# sql= "SELECT * FROM orders "
rows = cur.fetchall()

for r in rows:
  alldata =(f"  \t{r[0]},   \t{r[1]},    \t{r[2]},  \t{r[3]},   \t{r[4]}, \t{r[5]}, \t{r[12]} ")
  print (alldata)
print("------------------------------------------------------------------------------------------------------------------")

df = pd.read_sql('SELECT * FROM orders ', con=conn)
df.to_json()
print(df)


#  --------------------table C -------------------------


cur = conn.cursor() 
cur.execute("SELECT * FROM order_details")                               
rows = cur.fetchall()

for r in rows:
  alldata =(f"  \t{r[0]},   \t{r[1]},  \t{r[2]},  \t{r[3]},   \t{r[4]}, \t{r[5]} ")
  print (alldata)
print("------------------------------------------------------------------------------------------------------------------")

df = pd.read_sql('SELECT * FROM order_details ', con=conn)
df.to_json()
print(df)


#  -------------------table D -----------------------


cur = conn.cursor() 
cur.execute("SELECT * FROM products")                               
# sql= "SELECT * FROM orders "
rows = cur.fetchall()

for r in rows:
  alldata =(f"  \t{r[0]},   \t{r[1]},    \t{r[2]},  \t{r[3]},   \t{r[4]}, \t{r[5]}, \t{r[12]} ")
  print (alldata)
print("-----------------------------------------------------------------------------------------------------------------")

df = pd.read_sql('SELECT * FROM products ', con=conn)
df.to_json()
print(df)




#-------------------------table Result ----------------------------

cur = conn.cursor() 
cur.execute("SELECT * FROM resultEmp")
sql = "SELECT employees.last_name, employees.id, orders.order_date, orders.shipped_date, orders.shipping_fee, order_details.quantity, order_details.unit_price, products.product_name FROM order_details JOIN products ON order_details.product_id = products.id JOIN orders ON order_details.id = orders.id JOIN employees ON employees.id = orders.employee_id"
df = pd.read_sql(sql, con=conn) 
df.to_json()
print(df)

cur.close()
conn.close()