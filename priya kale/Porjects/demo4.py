import json
import mysql.connector
import psycopg2

conn =  mysql.connector.connect(user='root', password='root',
                          host='localhost', database='northwind')      

connpg =  psycopg2.connect(user='gismaster', password='first#1234',
                          host='aim-postgredb.caleitwp8flw.us-east-2.rds.amazonaws.com', database='northwind')                                                                     
cur = conn.cursor()
connpg.autocommit = True
curpg = connpg.cursor()

 
cur.execute("SELECT * FROM customers")
rows = cur.fetchall()
# for r in rows:
#   alldata =(f"  \t{r[0]},\t{r[1]},   \t{r[2]},    \t{r[3]}, ")
#   print (alldata)

  #s = json.dumps(alldata)
  #print(s)
# with open("data.txt","w") as f: #create file (step1)
#       f.write(s)
 
# with open("demo.json","w") as f: #create file
#       json.dump(alldata,f)

# with open("demo.json","r") as f: #read file
#       data = json.load(f)
#       print(data)
     
sd = curpg.execute("SELECT * FROM customer")
rowspg = curpg.fetchone()
print(rowspg)
rowspg = curpg.fetchall()
print(rowspg)
# s=curpg.execute("INSERT INTO customer (id) VALUES (1);")
# print(s)
print(sd)


connpg.commit()
connpg.close()

