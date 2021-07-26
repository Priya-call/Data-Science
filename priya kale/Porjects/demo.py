import json
import csv

with open("northwindPro.csv", "r") as f:
    reader = csv.reader(f)
    data ={"name":[]}
    for row in reader:
        data ["name"].append({
            "ComponyName":row[0],
            "ContactTitle": row[1],
            "Address": row[2],
            "City": row[3],
            "PostalCode": row[4],
            "Country": row[5],
            "Phone": row[6],
            "Fax": row[7],})
        print (data)

with open("name.json","w") as f:
    json.dump(data,f,indent=4)

data = json.load(open("name.json")) #for read json file
print(type(data))          