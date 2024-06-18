# %%
import hashlib
import urllib.parse
import xml.etree.ElementTree as ET

import pandas
import requests

endpoint = "https://crossmall.jp/"
secret_key = "TKe5dru05FZ1"
company_code = "4816"


def request_crossmall(path: str, params: dict, response_root: str) -> str:
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

    # stock値を取得
    stock_value = root.find(f".//{response_root}")
    if stock_value is not None:
        return stock_value.text
    else:
        return f"{response_root}要素が見つかりませんでした"


test_jan_code = 4589859570040
params = {
    "account": company_code,
    "jan_code": test_jan_code,
}
print(request_crossmall("get_stock", params, "stock"))
