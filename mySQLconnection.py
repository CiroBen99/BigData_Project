import csv
import mysql.connector

# connect to a database hosted on AWS
db = mysql.connector.connect(
    host='accidentsdb.cz6av9bkf3xq.eu-west-3.rds.amazonaws.com',
    user='admin',
    password='bdtproject2022',
    database='accidentsDB'
)

cursor = db.cursor()

# create the database
# cursor.execute("CREATE DATABASE accidentsDB")

# create the table
#cursor.execute("CREATE TABLE accidents (id VARCHAR(255), number VARCHAR(255), year VARCHAR(255), coordx VARCHAR(255), "
               #"coordy VARCHAR(255), fumetto VARCHAR(255), x_gps VARCHAR(255), y_gps VARCHAR(255), WKT VARCHAR(255))")

# read the csv file
csv_data = csv.reader(open("C:\\Users\\cirob\\Desktop\\BDT-project\\dataset\\incidenti.csv"))
header = next(csv_data)
print(header)

# import the csv file into the table
for row in csv_data:
    cursor.execute(
        'INSERT INTO accidents (id, number, year, coordx, coordy, fumetto, x_gps, y_gps, WKT) VALUES (%s, %s ,%s, %s, '
        '%s, %s, %s, %s ,%s)', row)
db.commit()
