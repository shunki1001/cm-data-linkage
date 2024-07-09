# %%
import hashlib
import os
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import date

import requests
from dotenv import load_dotenv

load_dotenv()

endpoint = "https://crossmall.jp/"
secret_key = os.environ["SECRET_KEY"]
company_code = os.environ["COMPANY_CODE"]

today = date.today()


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
