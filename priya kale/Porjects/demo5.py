import  mysql.connector
import pandas as pd
import psycopg2
import json

# ----------------------table A ------------------------

conn =  mysql.connector.connect(user='root', password='root',
                          host='localhost', database='northwind')      
cur = conn.cursor()
cur.execute("SELECT * FROM customers")
rows = cur.fetchall()
# print (rows)
   
alldata = ''
for r in rows:
  alldata = alldata + ("{" + f"  \"id\":{r[0]},\"company\":\"{r[1]}\",\"first\":\"{r[2]}\",\"data\":\"{r[3]}\" " + "},")
         
print (alldata)
# print (json.dumps(alldata))

print("------------------------------------------------------------------------------------------------------------------")

# df = pd.read_sql('SELECT * FROM customers', con=conn)
# df.to_json()
# print(df)

#------------------json file------------------


# json_object = json.dumps(data)
# with open("demo.json","w") as outputFile: #read file
#       outputFile.write(json_object)

# with open('demo.json', 'w') as outfile:
#      json.dump(alldata, outfile, sort_keys = True, indent = 4,
#                ensure_ascii = False)
      
# print('outfile')
# print(outfile)
jsonFile = open("demo.json", "w")
jsonFile.write(alldata)

       

jsonFile.close()
conn.commit()
conn.close()