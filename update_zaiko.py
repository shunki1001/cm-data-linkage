# %%
import hashlib
import os
import re
import time
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import date, timedelta

import pandas as pd
import requests
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

endpoint = "https://crossmall.jp/"
secret_key = os.environ["SECRET_KEY"]
company_code = os.environ["COMPANY_CODE"]

today = date.today()

bq_client = bigquery.Client(project="doctor-ilcsi")


def request_crossmall(
    path: str, params: dict, response_root: str, item_root_list=[]
) -> dict:
    # URLをエンコード
    encoded_params = urllib.parse.urlencode(params)

    # 署名の生成
    def generate_md5_hash(input_string):
        # ハッシュオブジェクトを作成
        md5_hash = hashlib.md5()
        # 入力文字列をバイト型にエンコードしてハッシュオブジェクトに渡す
        md5_hash.update(input_string.encode("utf-8"))
        # ハッシュ値を16進数の文字列で取得
        return md5_hash.hexdigest()

    signing_string = encoded_params + secret_key
    signing = generate_md5_hash(signing_string)

    url = f"{endpoint}webapi2/{path}?{encoded_params}&signing={signing}"
    try:
        response = requests.get(url=url)
    except:
        raise ConnectionError(f"CrossmallのAPIでエラー発生。params: {params}")
    if response.status_code != 200:
        raise ConnectionError(f"CrossmallのAPIでエラー発生。params: {params}")

    root = ET.fromstring(response.text)

    if item_root_list == ["None"]:
        # stock値を取得
        stock_value = root.find(f".//{response_root}")
        if stock_value is not None:
            return stock_value.text
        else:
            return f"{response_root}要素が見つかりませんでした"
    else:
        response_list = []
        for item in root.findall(f".//{response_root}"):
            temp_list = []
            for item_root in item_root_list:
                name = item.find(item_root).text
                temp_list.append({item_root: name})
            merged_dict = {}
            for d in temp_list:
                merged_dict.update(d)
            response_list.append(merged_dict)
        return response_list


def main(args):
    # 商品情報の一覧を取得（アイテムコードのみ）
    first_item_list = request_crossmall(
        "get_item",
        {"account": company_code, "condition": 1, "sort_type": 0},
        "Result",
        ["item_code"],
    )
    item_list = list(first_item_list)
    # 商品数が100以上の場合
    if len(first_item_list) > 99:
        temp_item_list = request_crossmall(
            "get_item",
            {
                "account": company_code,
                "item_code": first_item_list[-1],
                "condition": 1,
                "sort_type": 0,
            },
            "Result",
            ["item_code"],
        )
        item_list += temp_item_list
        while len(temp_item_list) > 99:
            time.sleep(0.5)
            temp_item_list = request_crossmall(
                "get_item",
                {
                    "account": company_code,
                    "item_code": temp_item_list[-1],
                    "condition": 1,
                    "sort_type": 0,
                },
                "Result",
                ["item_code"],
            )
            item_list += temp_item_list
    # 各商品ごとの詳細な情報をそれぞれ取得
    df_sku_item = pd.DataFrame()
    for item in item_list:
        temp_sku_list = request_crossmall(
            "get_item_sku",
            {"account": company_code, "item_code": item["item_code"]},
            "Result",
            [
                "item_code",
                "attribute1_code",
                "attribute1_name",
                "attribute2_code",
                "attribute2_name",
                "item_sku_code",
                "order_type",
                "order_point",
                "safety_stock",
                "lead_time",
                "order_lot",
                "keep_stock",
                "stock_unlimit_flag",
                "jan_code",
            ],
        )
        df_sku_item = pd.concat([df_sku_item, pd.DataFrame(temp_sku_list)]).reset_index(
            drop=True
        )
        time.sleep(0.5)
    # 1. set品を識別
    query = """
        SELECT distinct(parent_item_sku_code) FROM `doctor-ilcsi.dl_crossmall.set_item`
    """

    query_job = bq_client.query(query)  # APIリクエストの送信
    results = query_job.result()  # 結果を取得

    # クエリ結果をDataFrameに変換
    df_set_item = results.to_dataframe()

    df_zaiko_request = df_sku_item[
        ~df_sku_item["item_sku_code"].isin(df_set_item["parent_item_sku_code"])
    ]
    df_zaiko_request_filtered = df_zaiko_request[
        df_zaiko_request["jan_code"].notna()
    ].reset_index(drop=True)
    # 2. 在庫情報を取得
    zaiko_list = []
    for index, row in df_zaiko_request_filtered.iterrows():
        zaiko = request_crossmall(
            "get_stock",
            {"account": company_code, "sku_code": row["item_sku_code"]},
            "Result",
            [
                "item_cd",
                "attribute1_code",
                "attribute1_name",
                "attribute2_code",
                "attribute2_name",
                "stock",
            ],
        )
        zaiko[0].update({"cm_item_sku_code": row["item_sku_code"]})
        zaiko_list.append(zaiko[0])
    updated_df = pd.DataFrame(zaiko_list)
    updated_df["partition_date"] = today

    # BQに連携
    bigquery_job = bq_client.load_table_from_dataframe(
        updated_df,
        "doctor-ilcsi.dl_crossmall.zaiko",
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="partition_date"
            ),
        ),
    ).result()

    return "200"
