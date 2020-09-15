# import the PostgreSQL adapter for Python

import psycopg2

# Connect to the PostgreSQL database server

postgresConnection = psycopg2.connect("dbname=deneme4 user=postgres password='selimpinarbasi'")

# Get cursor object from the database connection

cursor = postgresConnection.cursor()

name_Table = "RANDOMNUMBERS"
name_Table2 = "RANDOMSTRINGS"
name_Table3 = "RANDOMNUMBERS2"
name_Table4 = "RANDOMNUMBERSDF"
# Create table statement

sqlCreateTable = "create table " + name_Table + " (ID INT, FOUR_DIGITS INT, FIVE_DIGITS INT, SIX_DIGITS INT, SEVEN_DIGITS INT);"
sqlCreateTable2 = "create table " + name_Table2 + " (ID INT, FOUR_DIGITS CHAR(10), FIVE_DIGITS CHAR(10), SIX_DIGITS CHAR(10), SEVEN_DIGITS CHAR(10));"
sqlCreateTable3 = "create table " + name_Table3 + " (ID INT, FOUR_DIGITS INT, FIVE_DIGITS INT, SIX_DIGITS INT, SEVEN_DIGITS INT);"
sqlCreateTable4 = "create table " + name_Table4 + " (ID INT, FOUR_DIGITS INT, FIVE_DIGITS INT, SIX_DIGITS INT, SEVEN_DIGITS INT);"



# Create a table in PostgreSQL database

cursor.execute(sqlCreateTable4)

postgresConnection.commit()

# Get the updated list of tables

sqlGetTableList = "SELECT table_schema,table_name FROM information_schema.tables where table_schema='test' ORDER BY table_schema,table_name ;"

# sqlGetTableList = "\dt"


# Retrieve all the rows from the cursor

cursor.execute(sqlGetTableList)

tables = cursor.fetchall()

"""
import psycopg2

#Establishing the connection
conn = psycopg2.connect(
   database="mydb", user='postgres', password='selimpinarbasi', host='127.0.0.1', port= '5432'
)
#Creating a cursor object using the cursor() method
cursor = conn.cursor()

#Doping EMPLOYEE table if already exists.
cursor.execute("DROP TABLE IF EXISTS EMPLOYEE")

#Creating table as per requirement
sql ='''CREATE TABLE RANDOMNUMBERS(
    FOUR_DIGITS INT,
    FIVE_DIGITS INT,
    SIX_DIGITS INT,
    SEVEN_DIGITS INT
)'''
sql ='''CREATE TABLE RANDOMSTRINGS(
    FOUR_DIGITS CHAR(10),
    FIVE_DIGITS CHAR(10),
    SIX_DIGITS CHAR(10),
    SEVEN_DIGITS CHAR(10)
)'''
cursor.execute(sql)
print("Table created successfully........")

#Closing the connection
conn.close()"""