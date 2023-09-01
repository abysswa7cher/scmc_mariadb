import base64
import pandas as pd
import mplfinance as fplt
import io
import IPython.display as IPydisplay
from datetime import datetime
from db_module import MarketDB

def construct_price_timeframe(db, table_name, date):

    if date is not None:
        if type(date) == tuple:
            year = date[0] if len(date) > 0 else "*"
            month = date[1] if len(date) > 1 else "*"
            day = date[2] if len(date) > 2 else "*"
            hour = date[3] if len(date) > 3 else "*"

    query = "select price, quantity, id, year, month, day, hour, minutes from {}".format(table_name)
    query = (query + (" where year='{}'" .format(year)   if year  != "*" else "") + 
                     (" and month='{}'"  .format(month)  if month != "*" else "") + 
                     (" and day='{}'"    .format(day)    if day   != "*" else "") + 
                     (" and hour='{}'"   .format(hour)   if hour  != "*" else ""))
    
    df = pd.read_sql(query, db.connection, index_col=None, parse_dates=True, dtype={"year": int, "month": int, "day": int, "hour": int})
    
    return df

def parse_table_to_df(db, table_name, date):

    df = construct_price_timeframe(db, table_name, date)

    df.drop_duplicates(["id"], keep="first", inplace=True)
    df.sort_values(by=["month", "day"], ascending=[True, True])

    day_sets = list()
    for day in df["day"].drop_duplicates().tolist():
        day_set = df.loc[df["day"]==day].sort_values(by="hour").reset_index()
        day_sets.append(day_set)

    monthly_df = pd.DataFrame(index=None)


    for i, set in enumerate(day_sets):
        hours = sorted(set["hour"].drop_duplicates().tolist())
        daily_df = pd.DataFrame(index=None)

        for j, hr in enumerate(hours):
            current_hour_set = set.loc[set["hour"]==hr]
            current_hour_set.sort_values(by="minutes", inplace=True)
        
            hourly_data = {}
            
            if j > 0:
                open = daily_df.iloc[-1]["Close"]
            else:
                open = current_hour_set.iloc[0]["price"]

            high = current_hour_set["price"].max()
            low = current_hour_set["price"].min()
            close = current_hour_set.iloc[-1]["price"]
            
            year = set['year'][0]

            month = set['month'][0]
            month = "0" + str(month) if 0 <= month <= 9 else str(month)

            day = set['day'][0]
            day = "0" + str(day) if 0 <= day <= 9 else str(day)

            hour = hr
            hour = ("0" + str(hour)) if 1 <= hour <= 9 else ("00" if hour == 0 else str(hour))

            minutes = current_hour_set['minutes'].max()                                                           #a = 1 if i < 100 else (2 if i > 100 else 0)
            minutes = ("0" + str(minutes)) if 1 <= minutes <= 9 else ("00" if minutes == 0 else str(minutes))

            date = pd.DatetimeIndex([pd.to_datetime(f"{year}{month}{day}{hour}{minutes}00", format='%Y%m%d%H%M%S')])
            vol = 0
            for i in current_hour_set["quantity"].values:
                vol += i

            hourly_data["Open"] = open
            hourly_data["High"] = high
            hourly_data["Low"] = low
            hourly_data["Close"] = close
            hourly_data["Date"] = date
            hourly_data["Volume"] = vol

            hourly_df = pd.DataFrame(hourly_data)
            hourly_df.set_index("Date", inplace=True)

            daily_df = pd.concat([daily_df, hourly_df])

        monthly_df = pd.concat([monthly_df, daily_df])
    print(monthly_df)
    return monthly_df

def draw_chart(db, table_name=None, date=None):
    db.connect("mariadb")

    while True:
        if table_name is None or type(table_name) != str or table_name == "":
            table_name = input("Enter resource name: ")
        else: break
    
    while True:
        if date is None or type(date) != tuple:
            if type(date) == str:
                date = tuple(map(int, date.split('-')))
            else:
                date = tuple(map(int, input("Enter YYYY-MM-DD to select a date: ").split('-')))
        else: break

    df = parse_table_to_df(db, table_name, date)

    db.disconnect()

    #macd
    ewm_12 = df["Close"].ewm(span=12, adjust=False).mean(),
    ewm_26 = df["Close"].ewm(span=26, adjust=False).mean()
    macd = ewm_12-ewm_26.mean()
    df["MACD"] = macd[0]

    #rsi
    delta = df['Close'].diff()
    period = 14
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    df['RSI'] = rsi
#region
    line33=[0.333 for x in range(0, len(df.index))]
    line33=pd.DataFrame([line33]).T
    line77=[0.777 for x in range(0, len(df.index))]
    line77=pd.DataFrame([line77]).T
    print(line33)
    print(line77)
#endregion
    ap0 = [fplt.make_addplot(df["MACD"], color="b", panel=1, ylabel="MACD"),
           fplt.make_addplot(df["RSI"], color="g", panel=2, ylabel="RSI"),
           fplt.make_addplot(line33, panel=2, color='b'),
           fplt.make_addplot(line77, panel=2, color='r')]
    
    df_date = df.index[-1]

    buffer = io.BytesIO()
    img_name = f"{table_name} ({df_date})"
    fplt.plot(  df, addplot=ap0,
                type='candle',
                style='yahoo',
                title=img_name,
                ylabel='Price ($)', 
                mav=(3, 9, 12),
                tight_layout=True, 
                volume=False, 
                figratio=(16, 9),
                figscale=0.8,
                savefig=buffer)
    
    buffer.seek(0)
    buf_png = base64.b64encode(buffer.getvalue()).decode()

    return "<img src='data:image/png;base64," + buf_png + "'/>"