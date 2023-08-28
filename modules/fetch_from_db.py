from db_module import MarketDB
import asyncio, httpx, datetime
import pandas as pd, numpy as np

db = MarketDB("mariadb")

table = db.get_table("fodder", "*")
days = table["day"].drop_duplicates(keep="first").values
table_name = input("Enter a table name: ")
selection = input("Enter YYYY-MM-DD to select a date: ")
res = tuple(map(int, selection.split('-')))

year_selected, month_selected, day_selected = res[0], res[1], res[2]

print(year_selected)
print(month_selected)
print(day_selected)

query = "select * from {} where year='{}' and month='{}' and day='{}'".format(table_name,
                                                                              year_selected, 
                                                                              month_selected, 
                                                                              day_selected)
df = pd.read_sql(query, db.connection, index_col=None)
print(df)
# for day in days:
#     if day == day_selected:
#         print(day)
