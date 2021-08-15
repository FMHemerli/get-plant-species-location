import gspread as gs
import pandas as pd
import numpy as np
import requests
import json
from oauth2client.service_account import ServiceAccountCredentials

pd.options.mode.chained_assignment = None  # default='warn'

scope = ["https://www.googleapis.com/auth/spreadsheets",
         "https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name('barter-analyser-957533c4bc5c.json', scope)
client = gs.authorize(creds)

sheet = client.open("lista oficial")
tab = sheet.worksheet("Plan1")
dataset = list(pd.DataFrame(tab.get_all_records()).loc[:149, ]['2'])
dataset = [x.split(None)[0] + " " + x.split(None)[1] for x in dataset]

url = "https://api.splink.org.br/records"
long_file = pd.DataFrame()

for i in dataset:
    print(i, end="")
    payload = {
        "Scientificname": i,
        "Format": "JSON",
        "ShowEmptyValues": "Yes",
        "fieldsCase": "Upper"
    }
    print(" .", end="")

    headers = {
        'Content-Type': 'application/json'
    }
    print(".", end="")

    response = requests.request("POST", url, headers=headers, data=json.dumps(payload))
    print(".", end="")

    response = json.loads(response.content.decode("UTF-8"))['result']
    print(".", end="")

    try:
        response = pd.DataFrame.from_dict(response).set_index('record_id')
        print(".", end="")

        response = response[response.DECIMALLONGITUDE != '']
        print(".", end="")

        coords = response[['SCIENTIFICNAME', 'DECIMALLONGITUDE', 'DECIMALLATITUDE']]
        print(".", end="")

    except KeyError:
        print("... SKIPPED")
        continue

    try:
        long_file += coords
    except:
        pass

    response.to_csv("./Tables/Entries/{}.csv".format(i), index=False)
    coords.to_csv("./Tables/Coords/{}.csv".format(i), index=False)

    print(" DONE")

long_file.to_csv("./Tables/coords.csv", index=False)
