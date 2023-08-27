import os
import pandas as pd
import datetime


def latest_bid_to_db(DB_object):

    files_list = os.listdir("data/json")
    resources = pd.read_csv("data/resources.csv", dtype={"id": int, "name": str})

    for file in files_list:
        path = f"data/json/{file}"
        dtype={"id": int, "kind": int, "quantity": int, "price": float, "posted": datetime.date} 
        df = pd.DataFrame()
        try:
            df = pd.read_json(path, dtype=dtype)
        except ValueError as ve:
            print("ValueError: json file was empty.")
        if not df.empty:
            df = df.drop(labels=["quality", "fees", "seller"], axis=1)
            last_ask = df.iloc[0]
                        #datetime.fromisoformat(last_ask['posted']).replace(tzinfo=None)
                        #datetime.combine(date.today(), datetime.time(datetime.now()))
                        #datetime

            data = {"kind":             int(last_ask['kind']),
                    "price":            float(last_ask['price']),
                    "quantity":         float(last_ask['quantity']),
                    "id":               int(last_ask['id']),
                    "date_posted":      datetime.datetime.fromisoformat(last_ask['posted']).replace(tzinfo=None),
                    "date_created":     datetime.datetime.now()}
            
            try:
                resource_name = resources.loc[resources["id"] == int(data["kind"])].values[0][1]
            except IndexError as e:
                print(e)
            
            if resource_name != None:
                table_name = f"{resource_name}"
                columns_create= "kind int, price float, quantity int, id int, date_posted datetime, date_created datetime"
                DB_object.create_table(table_name, columns_create)
                DB_object.dict_to_db(data, table_name)
        else:
            print(f"No bids found for {file[:-5]}")