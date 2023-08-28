import asyncio
import pandas as pd
import httpx
import datetime
timeout = 600.0

def fetch_market_data(db_object):
    market_api_url = "https://www.simcompanies.com/api/v3/market/0/"
    try:
        with open("data/resources.csv", "r", encoding="utf-8") as resources_csv:

            resources_df = pd.read_csv(resources_csv, sep=",", dtype={"id": int, "name": str}, engine="c")
            links = []
            for id, _ in resources_df.values:
                try:
                    request = f"{market_api_url}{id}/"
                    links.append(request)

                except Exception as e:
                    print(e)

            asyncio.run(_get_all(links, db_object))
        return -1
    except Exception as e:
        print(e)
        raise

async def _get_all(links, db_object):
    dtype={"id": int, "kind": int, "quantity": int, "price": float}
    resources = pd.read_csv("data/resources.csv", dtype=dtype, engine="c", parse_dates=True)

    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=5)) as client:
        
        tasks = [asyncio.create_task(_get_resource(link, client)) for link in links]

        for coro in asyncio.as_completed(tasks):
            result = await coro

            if result:
                data = _parse_resource(result)
                # print(data)
                _post_to_db(data, resources, db_object)
            else:
                print(f"Bad data for {result}")

async def _get_resource(link, client):
    print(f"Querying for '{link}'")
    resp = await client.get(link, timeout=timeout)
    print(f"Got data from {link}")

    try:
        resp.raise_for_status()

    except httpx.HTTPStatusError as err:
        if err.response.status_code != 200:
            return None
        raise
    else:
        return resp.json()

def _parse_resource(json):
    df = pd.DataFrame(json, index=None)

    if not df.empty:
        df = df.drop(labels=["quality", "fees", "seller"], axis=1)
        last_ask = df.iloc[0]
        date_posted = last_ask["posted"].replace("T", "").replace("-", "")[:15]

        year = date_posted[:4]
        month = date_posted[4:6]
        day = date_posted[6:8]
        time = (f"{date_posted[8:10]}:{date_posted[11:13]}")

        data = {"kind":             int(last_ask['kind']),
                "price":            float(last_ask['price']),
                "quantity":         float(last_ask['quantity']),
                "id":               int(last_ask['id']),
                "year":             year,
                "month":            month,
                "day":              day,
                "time":             time,
                "added_to_db":     datetime.datetime.now()}
        # print(data)
        return data

def _post_to_db(data, resources, DB_object):
    try:
        resource_name = resources.loc[resources["id"] == int(data["kind"])].values[0][1]
    except IndexError as e:
        print(e)
    
    if resource_name != None:
        table_name = f"{resource_name}"
        if not DB_object._table_exists(table_name):

            columns_create= ''' kind int, 
                                price float, 
                                quantity int, 
                                id int, 
                                year int, 
                                month int, 
                                day int, 
                                time varchar(8), 
                                added_to_db datetime'''
            DB_object.create_table(table_name, columns_create)

        DB_object.dict_to_db(data, table_name)
    else:
        print(f"No bids found for {resource_name}")