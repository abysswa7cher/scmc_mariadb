from db_module import MarketDB
import pandas as pd, numpy as np
from matplotlib.pyplot import title
import mplfinance as mpl

table_name = "fodder"#input("Enter a table name: ")
# selection = tuple(map(int, input("Enter YYYY-MM-DD to select a date: ").split('-')))

#year_selected, month_selected, day_selected = 2023, 8, 29#selection[0], selection[1], selection[2]

db = MarketDB("localhost")

def construct_price_timeframe(table_name, year="*", month="*", day="*", hour="*"):
    query_values = (table_name, year, month, day, hour)

    query = "select price, quantity, id from {}".format(table_name)
    query = (query + (" where year='{}'".format(year) if year != "*" else "") + 
            (" and month='{}'".format(month) if month != "*" else "") + 
            (" and day='{}'".format(day) if day != "*" else "") + 
            (" and hour='{}'".format(hour) if hour != "*" else ""))
    # query = query + (" and month='{}'".format(month) if month != "*" else "")
    # query = query + (" and day='{}'".format(day) if day != "*" else "")
    # query = query + (" and hour='{}'".format(hour) if hour != "*" else "")
    # print(query)
    df = pd.read_sql(query, db.connection, index_col=None, parse_dates=True)
    return df


def get_unique_orders_by_id(df):
    
    df.drop_duplicates(["id"], keep="first", inplace=True)
    df.sort_values(by="id", inplace=True, ignore_index=True)
    df.added_to_db = pd.DatetimeIndex(df.added_to_db)
    print(df)
    # price_open = 
    # price_close = 
    # price_high = 
    # price_low = 
    data = {"Open": df.iloc[0]["price"],
            "High": df["price"].max(),
            "Low": df["price"].min(),
            "Close": df.iloc[-1]["price"],
            "Start": df.iloc[0]["added_to_db"]}
    
    df_candle = pd.DataFrame(["data"], index=None)
    return(df_candle)

df = construct_price_timeframe(table_name=table_name, year=2023, month=8)
candle = get_unique_orders_by_id(df)

mpl.plot(candle,
         type="candle",
         mav=(3,6,9),
         title="f{table_name}",
         style="yahoo")