# %%
import os
import re
import time as pytime
from datetime import date, datetime, time, timedelta

import pandas as pd
import requests
from dotenv import load_dotenv

from request_crossmall import request_crossmall

load_dotenv()

company_code = os.environ["COMPANY_CODE"]

today = date.today()


# 日付部分を標準的な形式に変換する関数
def convert_to_date(message):
    # 正規表現パターンを定義
    patterns = [
        r"(\d+)月中旬",  # 例: '7月中旬'
        r"(\d+)月末",  # 例: '6月末'
        r"(\d+)月末～(\d+)月上旬",  # 例: '6月末～7月上旬'
        r"(\d+)月上旬",  # 例: '7月上旬'
        r"～(\d+)日",  # 例: '3～5日より発送予定となります'
        r"(\d+)日以内",  # 例: '7日以内に発送予定となります'
        r"(\d+)月初旬",  # 6月初旬より順次発送
        r"(\d+)月下旬",  # 6月下旬より順次発送
        r"(\d+)月上順",  # 6月上順
        r"(\d+)月上～中旬",  # 7月上～中旬
        r"(\d+)月中～下旬",  #
        r"(\d+)月下～上旬",  #
        r"(\d+)月上旬～(\d+)月中旬",  # 例:
        r"(\d+)月中旬～(\d+)月下旬",  # 例:
        r"(\d+)月下旬～(\d+)月上旬",  # 例:
        r"(\d+)月中順",  # 6月上順
        r"(\d+)月下順",  # 6月上順
    ]

    for pattern in patterns:
        match = re.search(pattern, message)
        if match:
            if pattern == patterns[0]:  # (\d+)月中旬
                month = int(match.group(1))
                return (date(today.year, month, 20)).strftime(format="%Y-%m-%d")
            elif pattern == patterns[1]:  # (\d+)月末
                month = int(match.group(1))
                return (date(today.year, month, 30)).strftime(format="%Y-%m-%d")
            elif pattern == patterns[2]:  # (\d+)月末～(\d+)月上旬
                end_month = int(match.group(2))
                return (date(today.year, end_month, 10)).strftime(format="%Y-%m-%d")
            elif pattern == patterns[3]:  # (\d+)月上旬
                month = int(match.group(1))
                return (date(today.year, month, 10)).strftime(format="%Y-%m-%d")
            elif pattern == patterns[4]:  # (\d+)～(\d+)日より発送予定となります
                end_day = int(match.group(1))
                return (today + timedelta(days=end_day)).strftime(format="%Y-%m-%d")
            elif pattern == patterns[5]:  # (\d+)日以内に発送予定となります
                days = int(match.group(1))
                return (today + timedelta(days=days)).strftime(format="%Y-%m-%d")
            elif pattern == patterns[6]:
                month = int(match.group(1))
                return (date(today.year, month, 10)).strftime(format="%Y-%m-%d")
            elif pattern == patterns[7]:
                month = int(match.group(1))
                return (date(today.year, month, 30)).strftime(format="%Y-%m-%d")
            elif pattern == patterns[8]:
                month = int(match.group(1))
                return (date(today.year, month, 10)).strftime(format="%Y-%m-%d")
            elif pattern == patterns[9]:
                month = int(match.group(1))
                return (date(today.year, month, 20)).strftime(format="%Y-%m-%d")
            elif pattern == patterns[10]:
                month = int(match.group(1))
                return (date(today.year, month, 30)).strftime(format="%Y-%m-%d")
            elif pattern == patterns[11]:
                month = int(match.group(1))
                return (date(today.year, month, 10)).strftime(format="%Y-%m-%d")
            elif pattern == patterns[12]:
                month = int(match.group(2))
                return (date(today.year, month, 30)).strftime(format="%Y-%m-%d")
            elif pattern == patterns[13]:
                month = int(match.group(2))
                return (date(today.year, month, 20)).strftime(format="%Y-%m-%d")
            elif pattern == patterns[14]:
                month = int(match.group(2))
                return (date(today.year, month, 10)).strftime(format="%Y-%m-%d")
            elif pattern == patterns[15]:
                month = int(match.group(1))
                return (date(today.year, month, 20)).strftime(format="%Y-%m-%d")
            elif pattern == patterns[16]:
                month = int(match.group(1))
                return (date(today.year, month, 30)).strftime(format="%Y-%m-%d")
        else:
            return (today + timedelta(days=7)).strftime(format="%Y-%m-%d")

    return None


def update_send_date():
    # 引当待ちフェーズ、かつ、Mark3がチェックされていない商品の一覧を取得
    first_order_list = request_crossmall(
        "get_order",
        {"account": company_code, "phase_name": "引当待ち", "check_mark3": 0},
        "Result",
        ["order_number"],
    )
    order_number_list = [order["order_number"] for order in first_order_list]

    if len(first_order_list) > 99:
        order_list = request_crossmall(
            "get_order",
            {
                "account": company_code,
                "order_number": first_order_list[-1]["order_number"],
                "condition": 1,
                "phase_name": "引当待ち",
                "check_mark3": 0,
            },
            "Result",
            ["order_number"],
        )
        order_number_list += [order["order_number"] for order in order_list]
        while len(order_list) > 99:
            pytime.sleep(0.5)
            order_list = request_crossmall(
                "get_order",
                {
                    "account": company_code,
                    "order_number": order_list[-1]["order_number"],
                    "condition": 1,
                    "phase_name": "引当待ち",
                    "check_mark3": 0,
                },
                "Result",
                ["order_number"],
            )
            order_number_list += [order["order_number"] for order in order_list]

    # すべてのorderに関して、
    for order_number in order_number_list:
        pytime.sleep(0.5)
        # 商品詳細情報を取得
        response = request_crossmall(
            "get_order_detail",
            {
                "account": company_code,
                "order_number": order_number,
            },
            "Result",
            ["lead_time_text"],
        )
        # 発送日に転記
        update_response = request_crossmall(
            "upd_order_phase",
            {
                "account": company_code,
                "order_number": order_number,
                "after_phase_name": "引当待ち",
                "delivery_date": convert_to_date(response[0]["lead_time_text"]),
                "delivery_date_update": 1,
            },
            "ResultStatus",
            ["UpdStatus"],
        )
        if update_response[0]["UpdStatus"] != "success":
            raise ConnectionError(
                f"発送日の更新で失敗しました。管理番号：{order_number}"
            )
        # Mark3をつける
        update_response = request_crossmall(
            "upd_order_check_mark",
            {
                "account": company_code,
                "order_number": order_number,
                "check_mark_type": 3,
                "check_mark_value": 1,
            },
            "ResultStatus",
            ["UpdStatus"],
        )
        print(f"{order_number} was succeed.")

    return "200"


def update_send_phase():
    # 発送待ちフェーズ、かつ、Mark2がチェックされていない商品の一覧を取得
    first_order_list = request_crossmall(
        "get_order",
        {"account": company_code, "phase_name": "発送待ち", "check_mark2": 0},
        "Result",
        [
            "order_number",
            "ship_zip",
            "ship_address1",
            "ship_address2",
            "order_memo",
            "order_option3",
        ],
    )
    orders = list(first_order_list)

    if len(first_order_list) > 99:
        order_list = request_crossmall(
            "get_order",
            {
                "account": company_code,
                "phase_name": "発送待ち",
                "order_number": first_order_list[-1]["order_number"],
                "check_mark2": 0,
            },
            "Result",
            [
                "order_number",
                "ship_zip",
                "ship_address1",
                "ship_address2",
                "order_memo",
                "order_option3",
            ],
        )
        orders += order_list
        while len(order_list) > 99:
            pytime.sleep(0.5)
            order_list = request_crossmall(
                "get_order",
                {
                    "account": company_code,
                    "phase_name": "発送待ち",
                    "order_number": order_list[-1]["order_number"],
                    "check_mark2": 0,
                },
                "Result",
                [
                    "order_number",
                    "ship_zip",
                    "ship_address1",
                    "ship_address2",
                    "order_memo",
                    "order_option3",
                ],
            )
            orders += order_list
    # すべてのorderに関して、
    for order in orders:
        phased_flag = True
        pytime.sleep(0.5)
        # 郵便番号と住所が一致しているかチェック
        zip_request = requests.get(
            url=f'https://zipcloud.ibsnet.co.jp/api/search?zipcode={order["ship_zip"]}'
        )
        zip_response = zip_request.json()["results"]
        addresses = [
            f"{entry['address1']}{entry['address2']}" for entry in zip_response
        ]
        for address in addresses:
            order_address = f'{order["ship_address1"]}{order["ship_address2"]}'
            check_address = []
            if address in order_address:
                check_address.append(True)
            else:
                check_address.append(False)
        if False in check_address:
            phased_flag = False
        # 離島フラグがあるかチェック
        if order["order_option3"] != "離島フラグ無":
            phased_flag = False
        # 備考に何か入力されているかチェック
        if order["order_memo"]:
            phased_flag = False

        # 特に問題なければ、フェーズを移動する。その後、チェックしたことが分かるように、Mark2にチェックを入れる
        if phased_flag:
            # 発送日に転記
            # 現在時刻で場合分け
            if datetime.now() < datetime.combine(today, time(15, 0)):
                update_response = request_crossmall(
                    "upd_order_phase",
                    {
                        "account": company_code,
                        "order_number": order["order_number"],
                        "after_phase_name": "RSL出荷登録待ち",
                        "delivery_date": today.strftime(format="%Y-%m-%d"),
                        "delivery_date_update": 1,
                    },
                    "ResultStatus",
                    ["UpdStatus"],
                )
                if update_response[0]["UpdStatus"] != "success":
                    raise ConnectionError(
                        f"発送日の更新で失敗しました。管理番号：{order['order_number']}"
                    )
            elif datetime.now() >= datetime.combine(today, time(15, 0)):
                update_response = request_crossmall(
                    "upd_order_phase",
                    {
                        "account": company_code,
                        "order_number": order["order_number"],
                        "after_phase_name": "RSL出荷登録待ち",
                        "delivery_date_update": 1,
                        "delivery_date": (today + timedelta(days=1)).strftime(
                            format="%Y-%m-%d"
                        ),
                    },
                    "ResultStatus",
                    ["UpdStatus"],
                )
                if update_response[0]["UpdStatus"] != "success":
                    raise ConnectionError(
                        f"発送日の更新で失敗しました。管理番号：{order['order_number']}"
                    )
        # Mark2をつける
        update_mark_response = request_crossmall(
            "upd_order_check_mark",
            {
                "account": company_code,
                "order_number": order["order_number"],
                "check_mark_type": 2,
                "check_mark_value": 1,
            },
            "ResultStatus",
            ["UpdStatus"],
        )
        print(f'{order["order_number"]}')
    return "200"


def main(args):
    print("発送日の転記開始")
    print(update_send_date())
    print("発送日の転記完了")

    print("RSLへの自動出荷連携処理開始")
    print(update_send_phase())
    print("RSLへの自動出荷連携処理完了")
    return "200"
