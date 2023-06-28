import pandas as pd
import os
import datetime
from ENVEA import Envea
from pandas import json_normalize
from logging_config import logger

now = datetime.datetime.now(tz=datetime.timezone.utc)
start = pd.to_datetime(datetime.datetime(2023, 2, 6), unit='ns', utc=True)
DH1 = datetime.timedelta(0, 0, 0, 0, 0, 1, 0)
workingDataFile = f"csv_data\\data_{datetime.datetime.now().strftime('%Y')}.csv"

# fetching last time of execution
with open("last.txt", "r") as f:
    last = pd.to_datetime(datetime.datetime.fromisoformat(f.read()), unit='ns', utc=True)

most_recent = pd.read_csv("csv_data/last_data.csv")

# fetching either 1h data or data since last execution
if now - last > datetime.timedelta(7):
    data_kwargs = {"from": start, "to": now}
else:
    data_kwargs = {"lastHours": 168}

# call the API
with Envea() as envea_dialoger:
    data_list = envea_dialoger.retrieve(ressource="restricted/v1/data", **data_kwargs)
    measures_list = envea_dialoger.retrieve('restricted/v1/measures')
    sites_list = envea_dialoger.retrieve('restricted/v1/sites')

# combine batched requests
if os.path.exists(os.path.join(os.path.dirname(__file__), workingDataFile)):
    data_points = pd.read_csv(workingDataFile, index_col=0)
else:
    data_points = pd.DataFrame(columns=["date", "id", "state", "validated", "value"])

data_df_list = [json_normalize(d.json()["data"], "base", meta="id") for d in data_list]
updated_points = pd.concat([pd.DataFrame(columns=["date", "id", "state", "validated", "value"])] + data_df_list)

updated_points = updated_points[updated_points["id"].str.contains('H2S')]

measures = pd.concat([json_normalize(m.json(), "measures") for m in measures_list])
sites = pd.concat([json_normalize(s.json(), "sites") for s in sites_list])

# cast datetime data
updated_points["date"] = pd.to_datetime(updated_points["date"], utc=True)

most_recent = most_recent.astype(updated_points.dtypes)

for i, row in updated_points.dropna().iterrows():
    ID = row['id']
    j = most_recent.query("id==@ID").index.values[0]
    if most_recent.loc[j, "date"] < row["date"] or pd.isna(most_recent.loc[j, "date"]):
        most_recent.iloc[j] = row.values

updated_points["date"] = updated_points["date"].dt.strftime("%d/%m/%Y %H:%M:%S")

data_points = pd.concat([data_points, updated_points])
data_points.drop_duplicates(inplace=True)

for col in ["lastDataDate", "startDate", "stopDate"]:
    measures[col] = pd.to_datetime(measures[col]).dt.strftime("%d/%m/%Y %H:%M:%S")

for col in ["startDate", "stopDate"]:
    sites[col] = pd.to_datetime(sites[col]).dt.strftime("%d/%m/%Y %H:%M:%S")

# order the columns for PowerBI
data_points = data_points.reindex(sorted(data_points.columns), axis=1)
measures = measures.reindex(sorted(measures.columns), axis=1)
sites = sites.reindex(sorted(sites.columns), axis=1)

add_header = not os.path.exists(
    os.path.dirname(__file__) + "\\" + f"csv_data\\data_{datetime.datetime.now().strftime('%Y')}.csv")

try:
    # write files to csv
    data_points.to_csv(f"csv_data\\data_{datetime.datetime.now().strftime('%Y')}.csv", mode="a", header=add_header)
    measures.to_csv(f"csv_data\\measures.csv")
    sites.to_csv(f"csv_data\\sites.csv")
    most_recent.to_csv("csv_data\\last_data.csv", index=False)
except Exception as e:
    logger.exception(e)
else:
    # writing now as last time of execution
    with open("last.txt", "w") as f:
        f.write(now.isoformat())
    logger.info("successful data recovery")
