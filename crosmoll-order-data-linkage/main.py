# %%
import os
import time
from datetime import date, datetime, timedelta

import pandas as pd
import pytz
from dotenv import load_dotenv
from google.cloud import bigquery
from request_crossmall import request_crossmall

load_dotenv()

company_code = os.environ["COMPANY_CODE"]

tokyo_tz = pytz.timezone("Asia/Tokyo")

# 現在のTokyoの日付と時刻を取得
tokyo_now = datetime.now(tokyo_tz)

# 日付部分だけを取得
today = tokyo_now.date()


def main(args):
    # 前日分の注文一覧を取得
    first_order_list = request_crossmall(
        "get_order",
        {
            "account": company_code,
            "updated_at_fr": (today - timedelta(days=1)).strftime(format="%Y-%m-%d"),
            "updated_at_to": today.strftime(format="%Y-%m-%d"),
        },
        "Result",
        ["order_number", "order_date", "phase_name", "updated_at"],
    )
    # APIの取得上限が100件
    if len(first_order_list) > 99:
        temp_order_list = request_crossmall(
            "get_order",
            {
                "account": company_code,
                "updated_at_fr": (today - timedelta(days=1)).strftime(
                    format="%Y-%m-%d"
                ),
                "updated_at_to": today.strftime(format="%Y-%m-%d"),
                "condition": 1,
                "order_number": first_order_list[-1]["order_number"],
            },
            "Result",
            ["order_number", "order_date", "phase_name", "updated_at"],
        )
        first_order_list += temp_order_list
        while len(temp_order_list) > 99:
            time.sleep(0.5)
            temp_order_list = request_crossmall(
                "get_order",
                {
                    "account": company_code,
                    "updated_at_fr": (today - timedelta(days=1)).strftime(
                        format="%Y-%m-%d"
                    ),
                    "updated_at_to": today.strftime(format="%Y-%m-%d"),
                    "condition": 1,
                    "order_number": temp_order_list[-1]["order_number"],
                },
                "Result",
                ["order_number", "order_date", "phase_name", "updated_at"],
            )
            first_order_list += temp_order_list
    # 注文詳細を取得
    order_detail_list = []
    # 構成品がある場合はその情報も取得
    component_detail_list = []
    for item in first_order_list:
        order_details = request_crossmall(
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
                "component_flag",
                "jan_cd",
            ],
        )
        for order_detail in order_details:
            # 構成品がある場合は別のdictで管理
            if order_detail["component_flag"] == "1":
                component_details = request_crossmall(
                    "get_order_component",
                    {
                        "account": company_code,
                        "order_number": order_detail["order_number"],
                        "line_no": order_detail["line_no"],
                    },
                    "Result",
                    [
                        "order_number",
                        "line_no",
                        "set_item_code",
                        "item_code",
                        "attribute1_code",
                        "attribute1_name",
                        "attribute2_code",
                        "attribute2_name",
                        "component_count",
                        "jan_cd",
                    ],
                )
                for component_detail in component_details:
                    component_detail_list.append(component_detail)
            order_detail["order_at"] = item["order_date"]
            order_detail["updated_at"] = item["updated_at"]
            order_detail["phase_name"] = item["phase_name"]
            order_detail_list.append(order_detail)
    # 注文情報の処理
    # Dataframeに変換＋前処理
    df_order = pd.DataFrame(order_detail_list)
    df_order = df_order.astype(str)
    df_order["partition_date"] = today
    df_order["order_at"] = pd.to_datetime(df_order["order_at"])
    df_order["updated_at"] = pd.to_datetime(df_order["updated_at"])
    # BQに連携
    bq_client = bigquery.Client(project="doctor-ilcsi")
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

    # セット品の処理
    # Dataframeに変換＋前処理
    df_component_item = pd.DataFrame(component_detail_list)
    df_component_item = df_component_item.astype(str)
    # BQに連携
    bigquery_job = bq_client.load_table_from_dataframe(
        df_component_item,
        "doctor-ilcsi.dl_crossmall.component_item_by_order",
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
        ),
    ).result()

    return "200"
