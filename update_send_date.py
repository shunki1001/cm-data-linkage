# %%
import hashlib
import os
import re
import time
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import date, timedelta

import pandas
import requests
from dotenv import load_dotenv

load_dotenv()

endpoint = "https://crossmall.jp/"
secret_key = os.environ["SECRET_KEY"]
company_code = os.environ["COMPANY_CODE"]

today = date.today()


def request_crossmall(
    path: str, params: dict, response_root: str, item_root="None"
) -> str:
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

    root = ET.fromstring(response.text)

    if item_root == "None":
        # stock値を取得
        stock_value = root.find(f".//{response_root}")
        if stock_value is not None:
            return stock_value.text
        else:
            return f"{response_root}要素が見つかりませんでした"
    else:
        response_list = []
        for item in root.findall(f".//{response_root}"):
            name = item.find(item_root).text
            response_list.append(name)
        return response_list


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


def main(args):
    # 引当待ちフェーズ、かつ、Mark1がチェックされている商品の一覧を取得
    first_order_list = request_crossmall(
        "get_order",
        {"account": company_code, "phase_name": "引当待ち", "check_mark3": 0},
        "Result",
        "order_number",
    )
    order_number_list = list(first_order_list)

    if len(first_order_list) > 99:
        order_list = request_crossmall(
            "get_order",
            {
                "account": company_code,
                "order_number": first_order_list[-1],
                "condition": 1,
                "phase_name": "引当待ち",
                "check_mark3": 0,
            },
            "Result",
            "order_number",
        )
        order_number_list += order_list
        while len(order_list) > 99:
            time.sleep(0.5)
            order_list = request_crossmall(
                "get_order",
                {
                    "account": company_code,
                    "order_number": order_list[-1],
                    "condition": 1,
                    "phase_name": "引当待ち",
                    "check_mark3": 0,
                },
                "Result",
                "order_number",
            )
            order_number_list += order_list

    # すべてのorderに関して、
    lead_time_text_list = []
    for order_number in order_number_list:
        time.sleep(0.5)
        # 商品詳細情報を取得
        response = request_crossmall(
            "get_order_detail",
            {
                "account": company_code,
                "order_number": order_number,
            },
            "lead_time_text",
        )
        lead_time_text_list.append(response)
        # 発送日に転記
        update_response = request_crossmall(
            "upd_order_phase",
            {
                "account": company_code,
                "order_number": order_number,
                "after_phase_name": "引当待ち",
                "delivery_date": convert_to_date(response),
                "delivery_date_update": 1,
            },
            "UpdStatus",
        )
        if update_response != "success":
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
            "UpdStatus",
        )
        print(f"{order_number} was succeed.")

    return "200"
