import  mysql.connector
#import pandas as pd
import psycopg2
import json

json_data = open("custRlt.json").read()
json_object =json.loads(json_data)

connpg =  psycopg2.connect(user='gismaster', password='first#1234',
                          host='aim-postgredb.caleitwp8flw.us-east-2.rds.amazonaws.com', database='northwind') 
curpg = connpg.cursor()
cur = connpg.cursor()

for item in json_object:
    first_name=item.get('first_name')
    id= item.get('id')
    order_date= item.get('order_date')
    shipped_date = item.get('shipped_date')
    shipping_fee= item.get('shipping_fee')
    Quantity= item.get('Quantity')
    unit_price= item.get('unit_price')
    product_name= item.get('product_name')
    v = cursor.execute("INSERT INTO json_file(first_name,id,order_date,shipped_date,shipping_fee,Quantity,unit_price,product_name) VALUE(#F,#I,#O,#S,#S,#Q,#U,#P)",(first_name,id,order_date,shipped_date,shipping_fee,Quantity,unit_price,product_name))
    print(v)
cur.close()
conn.close()
connpg.commit()
connpg.close()    
    