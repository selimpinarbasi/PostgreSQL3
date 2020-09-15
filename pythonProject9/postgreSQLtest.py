import psycopg2
import pandas as pd
import pandas.io.sql as psql
import timeit
import sqlalchemy

start = timeit.default_timer()

conn = psycopg2.connect(
   database="deneme4", user='postgres', password='selimpinarbasi', host='127.0.0.1', port= '5432'
)
engine = sqlalchemy.create_engine('postgresql://postgres:selimpinarbasi@localhost:5432/dataofmillions3')
#Setting auto commit false
conn.autocommit = True

#Creating a cursor object using the cursor() method
cursor = conn.cursor()

#sql_command = "SELECT COUNT(DISTINCT id) FROM RANDOMNUMBERS;"
#UNMATCHED ROWS

"""sql_command = "SELECT COUNT(*) FROM RANDOMNUMBERS " \
              "FULL OUTER JOIN RANDOMNUMBERSDF USING (SIX_DIGITS) " \
              "WHERE RANDOMNUMBERS.SIX_DIGITS IS NULL " \
              "OR RANDOMNUMBERSDF.SIX_DIGITS IS NULL "
              """

"""
ÇALIŞMAYA EN YAKIN
sql_command = "SELECT RANDOMNUMBERS.FOUR_DIGITS , RANDOMNUMBERSDF.FOUR_DIGITS " \
              "FROM RANDOMNUMBERS FULL OUTER JOIN RANDOMNUMBERSDF " \
              "ON RANDOMNUMBERS.FOUR_DIGITS=RANDOMNUMBERSDF.FOUR_DIGITS "
"""
"""sql_command = "SELECT COUNT(*) FROM RANDOMNUMBERS " \
              "FULL OUTER JOIN RANDOMNUMBERSDF USING (FIVE_DIGITS, FIVE_DIGITS) "
              """


"""
sql_command = "SELECT * FROM RANDOMNUMBERS " \
              "WHERE (six_digits, six_digits) NOT IN " \
              "(SELECT six_digits, six_digits FROM RANDOMNUMBERSDF);"
sql_command = "SELECT COUNT(*) FROM RANDOMNUMBERS"
sql_command2 = "SELECT COUNT(*) FROM RANDOMNUMBERSDF"
"""
data = pd.read_sql_table(
   'randomnumbers',
   engine,
   columns=['seven_digits'],
   coerce_float=True
)
data2 = pd.read_sql_table(
   'randomnumbersdf',
   engine,
   columns=['seven_digits'],
   coerce_float=True
)
#data = pd.read_sql(sql_command, conn)
#data2 = pd.read_sql(sql_command2, conn)
"""
print(data.columns.get_value(data, 2312))
print(data2.columns.get_value(data2, 2312))
"""

count_equal = 0
count_notequal = 0
i = 0
while i <= 21009:
   df_a = data.columns.get_value(data, i)
   df_b = data2.columns.get_value(data2, i)
   if(df_a == df_b):
      count_equal += 1
   if(df_a != df_b):
      count_notequal += 1
   i += 1

print("ayni", count_equal)
print("farkli", count_notequal)

stop = timeit.default_timer()
print("time is : ", stop - start, "seconds")

