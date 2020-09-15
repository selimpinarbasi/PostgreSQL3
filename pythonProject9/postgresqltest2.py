import psycopg2
import pandas as pd
import timeit
import sqlalchemy

start = timeit.default_timer()

engine = sqlalchemy.create_engine('postgresql://postgres:selimpinarbasi@localhost:5432/dataofmillions3')

data = pd.read_sql_table(
   'randomnumbers',
   engine,
   columns=['four_digits'],
   coerce_float=True
)
data2 = pd.read_sql_table(
   'randomnumbersdf',
   engine,
   columns=['four_digits'],
   coerce_float=True
)
#data = pd.read_sql(sql_command, conn)
#data2 = pd.read_sql(sql_command2, conn)
"""
print(data.columns.get_value(data, 2312))
print(data2.columns.get_value(data2, 2312))
"""

info = data.info
print("info ", info)

count_equal = 0
count_notequal = 0
i = 0
while i <= 2000999:
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

