# %%
import hashlib
import time
import urllib.parse
import xml.etree.ElementTree as ET

import pandas
import requests

endpoint = "https://crossmall.jp/"
secret_key = "TKe5dru05FZ1"
company_code = "4816"


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
    response = requests.get(url=url)

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


# 引当待ちフェーズ、かつ、Mark1がチェックされている商品の一覧を取得
first_order_list = request_crossmall(
    "get_order",
    {
        "account": company_code,
    },
    "Result",
    "order_number",
)

output_list = list(first_order_list)

if len(first_order_list) > 99:
    order_list = request_crossmall(
        "get_order",
        {"account": company_code, "order_number": first_order_list[-1], "condition": 1},
        "Result",
        "order_number",
    )
    output_list += order_list
    while len(order_list) > 99:
        time.sleep(1)
        order_list = request_crossmall(
            "get_order",
            {"account": company_code, "order_number": order_list[-1], "condition": 1},
            "Result",
            "order_number",
        )
        output_list += order_list

print(output_list)

# %%
# すべてのorderに関して、
# 商品詳細情報を取得
print(
    request_crossmall(
        "get_order_detail",
        {
            "account": company_code,
            "order_number": 270,
        },
        "lead_time_text",
    )
)

# 文字を正規表現で取得？リスト化？


# 発送日に転記
print(
    request_crossmall(
        "upd_order_phase",
        {
            "account": company_code,
            "order_number": 270,
            "after_phase_name": "",
            "delivery_date": "",
            "delivery_date_update": 1,
        },
        "lead_time_text",
    )
)
