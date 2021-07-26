import  mysql.connector
import pandas as pd
import psycopg2
import json


# -----------------------table A -------------------------

conn =  mysql.connector.connect(user='root', password='root',
                          host='localhost', database='northwind')                         
cur = conn.cursor()
 
cur.execute("SELECT * FROM customers")  

#sql= "SELECT * FROM customers "

rows = cur.fetchall()
for r in rows:
  alldata =(f"  \t{r[0]},\t{r[1]},   \t{r[2]},    \t{r[3]}, ")
  print (alldata)
print("---------------------------------------------------------------------------------------")

df = pd.read_sql('SELECT * FROM customers', con=conn)
df.to_json()
print(df)


# ---------------------------------------table B -------------------------------------

cur = conn.cursor() 
cur.execute("SELECT * FROM orders")                               
# sql= "SELECT * FROM orders "
rows = cur.fetchall()
for r in rows:
  alldata =(f"  \t{r[0]},   \t{r[1]},    \t{r[2]},  \t{r[3]},   \t{r[4]}, \t{r[5]} ")
  print (alldata)
print("----------------------------------------------------------------------------------------")

df = pd.read_sql('SELECT * FROM orders ', con=conn)
df.to_json()
print(df)



#  --------------------------------table C ---------------------------------

cur = conn.cursor()                              
#rows = cur.fetchall()
#data =(f"  \t{r[1]},  \t{r[2]},  \t{r[3]},  \t{r[4]}, \t{r[5]},  \t{r[6]},   \t{r[7]},   \t{r[8]},  \t{r[12]}")
sql = "SELECT customers.first_name,customers.last_name,customers.company,orders.employee_id,orders.customer_id,orders.shipped_date FROM customers JOIN orders ON customers.id = orders.customer_id";

# print (sql)
# print("-----------------------------------------------------------------------------------")
df = pd.read_sql(sql, con=conn) 
# df = pd.read_sql(sql,my_conn)
df.to_json()
print(df)

with open("name.json","w") as f: #create json file
    json.dump(alldata,f,indent=4)

data = json.load(open("name.json")) #for read json file
print(type(alldata))          

cur.close()
conn.close()