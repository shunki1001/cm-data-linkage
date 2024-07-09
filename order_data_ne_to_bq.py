# %%
import glob
import os

import pandas as pd
from google.cloud import bigquery

csv_files = glob.glob("./csv/*.csv")

# map.csvを読み込み
map_df = pd.read_csv("map.csv")

dataframes = []
for file in csv_files:
    df = pd.read_csv(file, encoding="shift-jis")
    filename = os.path.basename(file)
    df["filename"] = os.path.splitext(filename)[0]
    dataframes.append(df)

# すべてのDataFrameを一つのDataFrameに結合
combined_dataframe = pd.concat(dataframes, ignore_index=True)

# map.csvのfilenameをキーとしてcombined_dataframeにJANコードをマージ
combined_dataframe = pd.merge(
    combined_dataframe, map_df[["filename", "JAN_CODE"]], on="filename", how="left"
)

# 結果の表示
print(combined_dataframe)
combined_dataframe["JAN_CODE"] = combined_dataframe["JAN_CODE"].astype(int)

combined_dataframe = combined_dataframe[
    ["日付", "受注数", "受注金額", "filename", "JAN_CODE"]
].rename(columns={"日付": "order_at", "受注数": "amount", "受注金額": "sum_price"})

bq_client = bigquery.Client(project="doctor-ilcsi")
# %%
# BQに連携
bigquery_job = bq_client.load_table_from_dataframe(
    combined_dataframe,
    "doctor-ilcsi.dl_nextengine.old_order_info",
).result()
