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
    # 前日分の注文一覧を取得
    first_order_list = request_crossmall(
        "get_order",
        {
            "account": company_code,
            "order_date_fr": (today - timedelta(days=1)).strftime(format="%Y-%m-%d"),
            "order_date_to": (today - timedelta(days=1)).strftime(format="%Y-%m-%d"),
        },
        "Result",
        ["order_number", "order_date"],
    )
    # APIの取得上限が100件
    if len(first_order_list) > 99:
        temp_order_list = request_crossmall(
            "get_order",
            {
                "account": company_code,
                "order_date_fr": (today - timedelta(days=1)).strftime(
                    format="%Y-%m-%d"
                ),
                "order_date_to": (today - timedelta(days=1)).strftime(
                    format="%Y-%m-%d"
                ),
                "condition": 1,
                "order_number": first_order_list[-1]["order_number"],
            },
            "Result",
            ["order_number", "order_date"],
        )
        first_order_list += temp_order_list
        while len(temp_order_list) > 99:
            time.sleep(0.5)
            temp_order_list = request_crossmall(
                "get_order",
                {
                    "account": company_code,
                    "order_date_fr": (today - timedelta(days=1)).strftime(
                        format="%Y-%m-%d"
                    ),
                    "order_date_to": (today - timedelta(days=1)).strftime(
                        format="%Y-%m-%d"
                    ),
                    "condition": 1,
                    "order_number": temp_order_list[-1]["order_number"],
                },
                "Result",
                ["order_number", "order_date"],
            )
            first_order_list += temp_order_list
    # 注文詳細を取得
    order_detail_list = []
    for item in first_order_list:
        order_detail = request_crossmall(
            "get_order_detail",
            {"account": company_code, "order_number": item["order_number"]},
            "Result",
            [
                "order_number",
                "line_no",
                "item_code",
                "attribute1_code",
                "attribute1_name",
                "attribute2_code",
                "attribute2_name",
                "amount",
                "unit_price",
                "amount_price",
                "jan_cd",
            ],
        )
        order_detail[0]["order_at"] = item["order_date"]
        order_detail_list.append(order_detail[0])
    # Dataframeに変換＋前処理
    df_order = pd.DataFrame(order_detail_list)
    df_order = df_order.astype(str)
    df_order["partition_date"] = today
    df_order["order_at"] = pd.to_datetime(df_order["order_at"])
    # BQに連携
    bigquery_job = bq_client.load_table_from_dataframe(
        df_order,
        "doctor-ilcsi.dl_crossmall.order",
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="partition_date"
            ),
        ),
    ).result()
    return "200"
