from db_module import MarketDB
import pandas as pd
import mplfinance as fplt

db = MarketDB()
def construct_price_timeframe(table_name):
    selection = tuple(map(int, input("Enter YYYY-MM-DD to select a date: ").split('-')))

    user_input = {}

    try: 
        user_input["year"] = selection[0]
        print(f"year: {user_input['year']}")
    except: 
        print("You haven't selected a year, aborting.")
    try: 
        user_input["month"] = selection[1]
        print(f"month: {user_input['month']}")
    except: print("You haven't selected a month.")

    try: 
        user_input["day"] = selection[2]
        print(f"day: {user_input['day']}")
    except: print("You haven't selected a day.")

    kwargs_keys = user_input.keys()
    year = user_input["year"] if "year" in kwargs_keys else "*"
    month = user_input["month"] if "month" in kwargs_keys else "*"
    day = user_input["day"] if "day" in kwargs_keys else "*"
    hour = user_input["hour"] if "hour" in kwargs_keys else "*"

    print((year, month, day, hour))

    query = "select price, quantity, id, year, month, day, hour, minutes from {}".format(table_name)
    query = (query + (" where year='{}'" .format(year)   if year  != "*" else "") + 
                     (" and month='{}'"  .format(month)  if month != "*" else "") + 
                     (" and day='{}'"    .format(day)    if day   != "*" else "") + 
                     (" and hour='{}'"   .format(hour)   if hour  != "*" else ""))
    
    df = pd.read_sql(query, db.connection, index_col=None, parse_dates=True, dtype={"year": int, "month": int, "day": int, "hour": int})
    
    return df

def parse_table_to_df(table_name):

    while True:
        if table_name == "":
            print("Please enter a valid resource name: ")
        else: break

    df = construct_price_timeframe(table_name)

    df.drop_duplicates(["id"], keep="first", inplace=True)

    day_sets = list()
    for day in sorted(df["day"].drop_duplicates().tolist()):
        day_set = df.loc[df["day"]==day].sort_values(by="hour").reset_index()
        day_sets.append(day_set)



    monthly_df = pd.DataFrame(index=None)


    for i, set in enumerate(day_sets):
        hours = sorted(set["hour"].drop_duplicates().tolist())
        daily_df = pd.DataFrame(index=None) #columns=["Open", "High", "Low", "Close"], 

        for hr in hours:
            current_hour_set = set.loc[set["hour"]==hr]
            current_hour_set.sort_values(by="minutes", inplace=True)
        
            hourly_data = {}
            
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

def draw_chart():
    db.connect("mariadb")

    table_name = input("Enter resource name: ")
    df = parse_table_to_df(table_name)

    db.disconnect()

    fplt.plot(  df,
            type='candle',
            style='yahoo',
            title=f'{table_name}',
            ylabel='Price ($)', mav=(9, 21, 60), tight_layout=False, figratio=(10, 6), 
            savefig=dict(fname=f"{table_name}", dpi=300, pad_inches = 0.25))

draw_chart()