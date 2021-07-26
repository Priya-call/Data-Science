import  mysql.connector
import pandas as pd
import psycopg2
import json, os

# ----------------------table A ------------------------

conn =  mysql.connector.connect(user='root', password='root',
                          host='localhost', database='northwind')  

    
cur = conn.cursor()
cur.execute("SELECT * FROM customers")  
rows = cur.fetchall()
for r in rows:
  alldata =(f"  \t{r[0]},\t{r[1]},   \t{r[2]},    \t{r[3]}, ")
  # print (alldata)
# print("------------------------------------------------------------------------------------------------------------------")

df = pd.read_sql('SELECT * FROM customers', con=conn)
df.to_json()
# print(df)


# ----------------------table B ------------------------


cur = conn.cursor() 
cur.execute("SELECT * FROM orders")                               
# sql= "SELECT * FROM orders "
rows = cur.fetchall()

for r in rows:
  alldata =(f"  \t{r[0]},   \t{r[1]},    \t{r[2]},  \t{r[3]},   \t{r[4]}, \t{r[5]}, \t{r[12]} ")
  # print (alldata)
# print("------------------------------------------------------------------------------------------------------------------")

df = pd.read_sql('SELECT * FROM orders ', con=conn)
df.to_json()
# print(df)


#  --------------------table C -------------------------


cur = conn.cursor() 
cur.execute("SELECT * FROM order_details")                               
rows = cur.fetchall()

for r in rows:
  alldata =(f"  \t{r[0]},   \t{r[1]},  \t{r[2]},  \t{r[3]},   \t{r[4]}, \t{r[5]} ")
  # print (alldata)
# print("------------------------------------------------------------------------------------------------------------------")

df = pd.read_sql('SELECT * FROM order_details ', con=conn)
df.to_json()
# print(df)


#  -------------------table D -----------------------


cur = conn.cursor() 
cur.execute("SELECT * FROM products")                               
# sql= "SELECT * FROM orders "
rows = cur.fetchall()

for r in rows:
  alldata =(f"  \t{r[0]},   \t{r[1]},    \t{r[2]},  \t{r[3]},   \t{r[4]}, \t{r[5]}, \t{r[12]} ")
  # print (alldata)
# print("-----------------------------------------------------------------------------------------------------------------")

df = pd.read_sql('SELECT * FROM products ', con=conn)
df.to_json()
# print(df)




#  -------------------------table Result ----------------------------

connpg =  psycopg2.connect(user='gismaster', password='first#1234',
                          host='aim-postgredb.caleitwp8flw.us-east-2.rds.amazonaws.com', database='northwind') 

connpg.autocommit = True
curpg = connpg.cursor()

cur = conn.cursor()
cur.execute("SELECT * FROM result")
 
#sql = "SELECT customers.first_name, customers.id, orders.order_date, orders.shipped_date, orders.shipping_fee, order_details.quantity, order_details.unit_price, products.product_name FROM order_details JOIN products ON order_details.product_id = products.id JOIN orders ON order_details.id = orders.id JOIN customers ON customers.id = orders.customer_id"
#sql = "SELECT customers.first_name,customers.last_name,customers.company,orders.employee_id,orders.customer_id,orders.shipped_date FROM customers JOIN orders ON customers.id = orders.customer_id";
rows = cur.fetchall()   
alldata = ''
# print("===========================================================================")
# print('DB: ',rows)
# print("===========================================================================")

rowLength = len(rows)
rowRange = rowLength - 1
a = []

for item in range(0, rowLength):
    # if(item == rowRange):    
      #alldata = alldata + ("{" + f"  \"first_name\":\"{rows[0][0]}\",\"id\":\"{rows[1][1]}\",\"order_date\":\"{rows[2][2]}\",\"shipped_date\":\"{rows[3][3]}\", \"shipping_fee\":\"{rows[4][4]}\",\"Quantity\":\"{rows[5][5]}\",\"unit_price\":\"{rows[6][6]}\",\"product_name\":\"{rows[7][7]}\"" + "},")
      alldata =( "{"  +f"  \"first_name\":\"{r[0]}\",   \"id\":\"{r[1]}\",  \"order_date\":\"{r[2]}\",  \"shipped_date\":\"{r[3]}\",   \"shipping_fee\":\"{r[4]}\", \"Quantity\":\"{r[5]}\", \"unit_price\":\"{r[6]}\" ,\"product_name\":\"{r[7]}\",  " + "},")

      #alldata = alldata + ( "{" + f" \"first_name\":\"{r[0]}\",\"id\":\"{r[1]}\",\"order_date\":\"{r[2]}\",\"shipped_date\":\"{r[3]}\", \"shipping_fee\":\"{r[4]}\",\"Quantity\":\"{r[5]}\",\"unit_price\":\"{r[6]}\",\"product_name\":\"{r[7]}\"" + "}" )
      # b = a + ("{" + f" \"first_name\":\"{r[0]}\",\"id\":\"{r[1]}\",\"order_date\":\"{r[2]}\",\"shipped_date\":\"{r[3]}\", \"shipping_fee\":\"{r[4]}\",\"Quantity\":\"{r[5]}\",\"unit_price\":\"{r[6]}\",\"product_name\":\"{r[7]}\"" + "}")
      data = alldata[:-1]
      print('alldata', data)    
      
    # else:
    #   alldata = alldata + ("{" + f"  \"first_name\":\"{r[0]}\",\"id\":\"{r[1]}\",\"order_date\":\"{r[2]}\",\"shipped_date\":\"{r[3]}\", \"shipping_fee\":\"{r[4]}\",\"Quantity\":\"{r[5]}\",\"unit_price\":\"{r[6]}\",\"product_name\":\"{r[7]}\"" + "},")
          


# for r in rows:
#   #print(r)
  
#   # alldata = alldata + ("{" + f"  \"first_name\":\"{r[0]}\",\"id\":\"{r[1]}\",\"order_date\":\"{r[2]}\",\"shipped_date\":\"{r[3]}\", \"shipping_fee\":\"{r[4]}\",\"Quantity\":\"{r[5]}\",\"unit_price\":\"{r[6]}\",\"product_name\":\"{r[7]}\", " + "},")
#   alldata = alldata + ("{" + f"  \"first_name\":\"{r[0]}\",\"id\":\"{r[1]}\",\"order_date\":\"{r[2]}\",\"shipped_date\":\"{r[3]}\", \"shipping_fee\":\"{r[4]}\",\"Quantity\":\"{r[5]}\",\"unit_price\":\"{r[6]}\",\"product_name\":\"{r[7]}\", " + "},")
  

# df = pd.read_sql(sql, con=conn) 
# df.to_json()
# print(df)

jsonFile = open("custRlt.json", "w")
s=jsonFile.write(data)    
      

#rowspg = curpg.fetchall()
#sb = ("custRlt.json")
#print(sb)

#TableName = "CREATE TABLE result ( file VARCHAR(255), id int);" 
# curpg.execute(TableName)

# TableName = "CREATE TABLE result(info_json)" # for import data in postgres
# "/set content =('custRlt.json')"
# "INSERT INTO result VALUES(:'content')"
# sd = curpg.execute("SELECT * FROM result")
# print(sd,"sd")

# p = ("INSERT INTO result(name,id,)VALUES('pia','1')" )
# print(p)

# tb = "CREATE TABLE rlt(" + f"  \"first_name\":{r[0]},\"id\":\"{r[1]}\",\"order_date\":\"{r[2]}\",\"shipped_date\":\"{r[3]}\" \"shipping_fee\":\"{r[4]}\",\"Quantity\":\"{r[5]}\",\"unit_price\":\"{r[6]}\",\"product_name\":\"{r[7]}\", " + ")"
# #p = ("INSERT INTO rlt(first_name,id,order_date,shipped_date,shipping_fee,Quantity,unit_price,product_name)VALUES('bella','1','21/2/2','23/1/11','2300','54','2300','meka')")
# print (tb)




jsonFile.close()
conn.commit()
cur.close()
conn.close()
connpg.commit()
connpg.close()

