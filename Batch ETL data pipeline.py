# Import libraries required for connecting to mysql and ibm_db2
import mysql.connector
import ibm_db
import logging
import sys
from sqlalchemy import create_engine

# Connect to MySQL
database = 'your database'
user = 'your username'
password = 'your password'
host = 'your hostname'
connection = mysql.connector.connect(user=user, password=password,host=host,database=database)
print ("Connected to MySQL database:",database, "as user:",user, "on host:",host)

# Create MySQL cursor
cursor = connection.cursor()

# Create table in MySQL
DROP_TABLE = "DROP TABLE IF EXISTS products"
CREATE_TABLE = """CREATE TABLE IF NOT EXISTS products(
rowid int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
product varchar(255) NOT NULL,
category varchar(255) NOT NULL
)"""

cursor.execute(DROP_TABLE)
cursor.execute(CREATE_TABLE)
print("Table created")

# Insert data into MySQL
SQL = """INSERT INTO products(product,category)
	 VALUES
	 ("Television","Electronics"),
	 ("Laptop","Electronics"),
	 ("Mobile","Electronics")"""

cursor.execute(SQL)
connection.commit()
print("records successfully inserted")

# Query data in MySQL
SQL = "SELECT * FROM products"
cursor.execute(SQL)
for row in cursor.fetchall():
	print(row)


# Connect to DB2
dsn_hostname = "your hostname"
dsn_uid = "your username"
dsn_pwd = "your password"
dsn_port = "32286"
dsn_database = "BLUDB"
dsn_driver = "{IBM DB2 ODBC DRIVER}"
dsn_protocol = "TCPIP"
# dsn_auth = "SERVER"
dsn_security = "SSL"

dsn = (
    "DRIVER={0};"
    "DATABASE={1};"
    "HOSTNAME={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};"
    "SECURITY={7};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd,dsn_security)

print(dsn)

# create connection
try:
    conn = ibm_db.connect(dsn, "", "")
    print ("Connected to database: ", dsn_database, "as user: ", dsn_uid, "on host: ", dsn_hostname)
except:
    print ("Unable to connect: ", ibm_db.conn_errormsg())


# Find out the last rowid on sales_data table from DB2 data warehouse
def get_last_rowid():
	SQL="SELECT rowid FROM sales_data order by rowid desc limit 1"
	stmt = ibm_db.exec_immediate(conn, SQL)
	tuple = ibm_db.fetch_tuple(stmt)
	return tuple[0]

last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)


# Obtain records from MySQL OLTP database with rowid greater than last_row_id in IBM Db2 Data warehouse 
def get_latest_records(last_row_id):
	latest_records=[]
	SQL = f"SELECT * FROM sales_data where rowid > {last_row_id} order by rowid"
	cursor.execute(SQL)
	for row in cursor.fetchall():
		latest_records.append(row)
	return latest_records

new_records = get_latest_records(last_row_id)
print("Number of New rows on staging datawarehouse = ", len(new_records))


# Insert new records from MySQL OLTP database into DB2 data warehouse.
def insert_records(records):
    for row in records:
        insertQuery = f"insert into sales_data(rowid, product_id, customer_id, quantity) values{row}"
        ibm_db.exec_immediate(conn, insertQuery)
    print("New rows inserted into production datawarehouse = ", len(records))
        
insert_records(new_records)



# Verify by checking total number of rows
verify= %sql select * from sales_data order by rowid
df=verify.DataFrame()
print(df.shape)


# disconnect from mysql warehouse
connection.close()

# disconnect from DB2 data warehouse
ibm_db.close(conn)


# End of program
