# Batch-ETL-data-pipeline using Python

## `Scenario`: A online store has an OLTP database (MySQL) where daily sales transactions are stored as orders are processed online. The company desires to extract daily data into a data warehouse (IBM Db2) so it could perform analytic queries and create reports too. It has requested that an ETL batch data pipeline be created with Python to automate this ETL process at the end of each business day.

## `Steps taken`
- A new Python file was created and prerequisite libraries for connecting to relational databases were imported e.g., mysql-connector, ibm_db2, sqlachemy
- Connections were created for the MySQL and IBM Db2 cloud databases and basic SELECT queries were executed to affirm successful connection
- A Python function was created to obtain the last rowid on the sales_data table in the data warehouse (IBM Db2 Cloud) and the rowid was saved as a Python variable
- Second Python function was created to obtain all records from the OLTP database (MySQL) with rowid greater than the last rowid from IBM Db2 data warehouse. These records were stored as tuples in a Python list.
- Third Python function was created to insert all new records (earlier stored in a Python list) from the  OLTP database (MySQL) into the data warehouse (IBM Db2)
- Basic SELECT queries like count were executed on the data warehouse (IBM Db2) to confirm success of the bulk inserts on the data warehouse.
- Finally, connections for both database were closed.
