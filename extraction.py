import pandas as pd
import os
import datetime
from ENVEA import ENVEA
from pandas import json_normalize
from logging_config import logger

now = datetime.datetime.now(tz=datetime.timezone.utc)
start = pd.to_datetime(datetime.datetime(2023, 2, 6), unit='ns', utc=True)
DH1 = datetime.timedelta(0, 0, 0, 0, 0, 1, 0)
workingDataFile = f"csv_data\\data_{datetime.datetime.now().strftime('%Y')}.csv"

# fetching last time of execution
with open("last.txt", "r") as f:
    last = pd.to_datetime(datetime.datetime.fromisoformat(f.read()), unit='ns', utc=True)

LAST = pd.read_csv("csv_data/last_data.csv")

# fetching either 1h data or data since last execution
if now - last > datetime.timedelta(7):
    data_kwargs = {"from": start, "to": now}
else:
    data_kwargs = {"lastHours": 168}

# call the API
with ENVEA() as envea:
    data_list = envea.retrieve(ressource="restricted/v1/data", **data_kwargs)
    measures_list = envea.retrieve('restricted/v1/measures')
    sites_list = envea.retrieve('restricted/v1/sites')

# combine batched requests
if os.path.exists(os.path.join(os.path.dirname(__file__), workingDataFile)):
    DATA = pd.read_csv(workingDataFile, index_col=0)
else:
    DATA = pd.DataFrame(columns=["date", "id", "state", "validated", "value"])

data_df_list = [json_normalize(d.json()["data"], "base", meta="id") for d in data_list]
UPDATED = pd.concat([pd.DataFrame(columns=["date", "id", "state", "validated", "value"])] + data_df_list)

UPDATED = UPDATED[UPDATED["id"].str.contains('H2S')]

MEASURES = pd.concat([json_normalize(m.json(), "measures") for m in measures_list])
SITES = pd.concat([json_normalize(s.json(), "sites") for s in sites_list])

# cast datetime data
UPDATED["date"] = pd.to_datetime(UPDATED["date"], utc=True)

LAST = LAST.astype(UPDATED.dtypes)

for i, row in UPDATED.dropna().iterrows():
    ID = row['id']
    j = LAST.query("id==@ID").index.values[0]
    if LAST.loc[j, "date"] < row["date"] or pd.isna(LAST.loc[j, "date"]):
        LAST.iloc[j] = row.values

UPDATED["date"] = UPDATED["date"].dt.strftime("%d/%m/%Y %H:%M:%S")

DATA = pd.concat([DATA, UPDATED])
DATA.drop_duplicates(inplace=True)

for col in ["lastDataDate", "startDate", "stopDate"]:
    MEASURES[col] = pd.to_datetime(MEASURES[col]).dt.strftime("%d/%m/%Y %H:%M:%S")

for col in ["startDate", "stopDate"]:
    SITES[col] = pd.to_datetime(SITES[col]).dt.strftime("%d/%m/%Y %H:%M:%S")

# order the columns for PowerBI
DATA = DATA.reindex(sorted(DATA.columns), axis=1)
MEASURES = MEASURES.reindex(sorted(MEASURES.columns), axis=1)
SITES = SITES.reindex(sorted(SITES.columns), axis=1)

add_header = not os.path.exists(
    os.path.dirname(__file__) + "\\" + f"csv_data\\data_{datetime.datetime.now().strftime('%Y')}.csv")

try:
    # write files to csv
    DATA.to_csv(f"csv_data\\data_{datetime.datetime.now().strftime('%Y')}.csv", mode="a", header=add_header)
    MEASURES.to_csv(f"csv_data\\measures.csv")
    SITES.to_csv(f"csv_data\\sites.csv")
    LAST.to_csv("csv_data\\last_data.csv", index=False)
except Exception as e:
    logger.exception(e)
else:
    # writing now as last time of execution
    with open("last.txt", "w") as f:
        f.write(now.isoformat())
    logger.info("successful data recovery")
