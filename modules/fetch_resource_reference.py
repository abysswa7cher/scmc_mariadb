import json
import pandas as pd
from urllib.request import urlopen

resources_api_url = "https://www.simcompanies.com/api/v4/en/0/encyclopedia/resources/"

def fetch_resource_reference():
    with open("data/resources.csv", "w+", encoding="utf-8"):
        response = urlopen(resources_api_url)
        json_object = json.loads(response.read())

        df = pd.DataFrame()

        for entry in json_object:
            data = {"id":   entry["db_letter"],
                    "name": (entry["name"].replace(" ", "_").replace("-", "_").lower())}
            df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)
            
        df.sort_values("id").to_csv("data/resources.csv", index=False)