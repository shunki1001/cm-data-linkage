# %%
import os
import time
from datetime import date, datetime, timedelta

import pandas as pd
import pytz
from dotenv import load_dotenv
from google.cloud import bigquery

# ChromeのWebDriverライブラリをインポート
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

load_dotenv(".env.yaml")


def main(args):

    # ブラウザーを起動
    global driver
    options = webdriver.ChromeOptions()
    # 以下は各種オプションを設定 なくても動作するものがほとんどです
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--single-process")
    options.add_argument('--proxy-server="direct://"')
    options.add_argument("--proxy-bypass-list=*")
    options.add_argument("--blink-settings=imagesEnabled=false")
    options.add_argument("--lang=ja")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--log-level=3")
    options.add_argument("--disable-logging")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36"
    )
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.page_load_strategy = "eager"

    # WEBブラウザとWEBドライバの場所を指定（GoogleCloudFunction用）
    options.binary_location = os.getcwd() + "/headless-chromium"
    driver = webdriver.Chrome(os.getcwd() + "/chromedriver", options=options)
    # ↑2行をコメントアウトして以下1行に置き換えるとWindowsPC等他の環境でも動きます。
    # driver = webdriver.Chrome(options=options)

    # 1. あるシステムにログイン
    driver.get("https://www.crossmall.jp/")  # ログインページのURLを指定
    time.sleep(1)
    # ログイン情報を入力
    username = driver.find_element(By.NAME, "mail")  # ユーザ名のinput要素を指定
    # password = driver.find_element(By.NAME, '1139bf73')  # パスワードのinput要素を指定
    password = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, "//form/p[2]/input"))
    )
    username.send_keys(os.getenv("USERNAME"))  # ユーザ名を入力
    password.send_keys(os.getenv("PASS"))  # パスワードを入力
    driver.find_element(
        By.NAME, "commit"
    ).click()  # ログインボタンをクリック（name属性が'login'の場合）

    tokyo_tz = pytz.timezone("Asia/Tokyo")
    # 現在のTokyoの日付と時刻を取得
    tokyo_now = datetime.now(tokyo_tz)
    # 日付部分だけを取得
    today = tokyo_now.date()

    # RSL在庫数のデータ取得
    update_rsl(today)

    # RSLへの入荷数のデータ取得
    update_income(today)

    driver.quit()

    return "200"


def update_rsl(today):

    driver.get("https://www.crossmall.jp/rfc_stocks/?mId=&umId=0")

    wait = WebDriverWait(driver, 60)
    element = wait.until(EC.invisibility_of_element_located((By.ID, "loadmask-1186")))

    # 検索条件
    input2 = driver.find_element(By.ID, f"reflected_flag")
    select = Select(input2)
    select.select_by_value("true")

    input2 = driver.find_element(By.ID, f"max_row")
    select = Select(input2)
    select.select_by_value("300")

    # 検索ボタン
    input2 = driver.find_element(
        By.XPATH, f'//*[@id="rfc_stocks_form"]/div[1]/div[2]/div/input[1]'
    )
    input2.click()

    time.sleep(5)

    # 在庫数とJANコードを取得
    stocks = []
    try:
        for index in range(1, 300, 1):
            stocks.append(
                {
                    "cm_item_sku_code": driver.find_element(
                        By.XPATH,
                        f'//*[@id="gridview-1175-body"]/tr[{index}]/td[{3}]/div',
                    ).text,
                    "cm_item_title": driver.find_element(
                        By.XPATH,
                        f'//*[@id="gridview-1175-body"]/tr[{index}]/td[{4}]/div',
                    ).text,
                    "cm_item_zokusei_name1": driver.find_element(
                        By.XPATH,
                        f'//*[@id="gridview-1175-body"]/tr[{index}]/td[{5}]/div',
                    ).text,
                    "cm_item_zokusei_name2": driver.find_element(
                        By.XPATH,
                        f'//*[@id="gridview-1175-body"]/tr[{index}]/td[{6}]/div',
                    ).text,
                    "logi_item_code": driver.find_element(
                        By.XPATH,
                        f'//*[@id="gridview-1175-body"]/tr[{index}]/td[{7}]/div',
                    ).text,
                    "logi_item_title": driver.find_element(
                        By.XPATH,
                        f'//*[@id="gridview-1175-body"]/tr[{index}]/td[{8}]/div',
                    ).text,
                    "jan_code": driver.find_element(
                        By.XPATH,
                        f'//*[@id="gridview-1175-body"]/tr[{index}]/td[{9}]/div',
                    ).text,
                    "cm_stock": driver.find_element(
                        By.XPATH,
                        f'//*[@id="gridview-1175-body"]/tr[{index}]/td[{10}]/div',
                    ).text,
                    "rsl_stock": driver.find_element(
                        By.XPATH,
                        f'//*[@id="gridview-1175-body"]/tr[{index}]/td[{11}]/div/a',
                    ).text,
                    "updated_at": driver.find_element(
                        By.XPATH,
                        f'//*[@id="gridview-1175-body"]/tr[{index}]/td[{12}]/div',
                    ).text,
                }
            )
    except:
        print(f"{index}まで成功")

    df_stock = pd.DataFrame(stocks)
    df_stock["jan_code"] = df_stock["jan_code"].astype(str)
    df_stock["cm_stock"] = df_stock["cm_stock"].astype(int)
    df_stock["rsl_stock"] = df_stock["rsl_stock"].astype(int)
    df_stock["partition_date"] = today

    bq_client = bigquery.Client(project="doctor-ilcsi")

    bigquery_job = bq_client.load_table_from_dataframe(
        df_stock,
        "doctor-ilcsi.dl_crossmall.rsl_zaiko_from_cm",
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="partition_date"
            ),
        ),
    ).result()


def update_income(today):
    # 入荷情報を取得
    driver.get("https://www.crossmall.jp/rfc_inbound_shedules/?mId=&umId=0")

    time.sleep(2)

    # 検索条件
    input2 = driver.find_element(By.ID, f"unfinished_flg")
    select = Select(input2)
    select.select_by_value("")

    input2 = driver.find_element(By.ID, f"date_from")
    input2.send_keys((today - timedelta(weeks=2)).strftime(format="%Y/%m/%d"))

    # 検索ボタン
    input2 = driver.find_element(
        By.XPATH, f'//*[@id="rfc_inbound_shedules_form"]/div[1]/div[2]/div/input[1]'
    )
    input2.click()

    time.sleep(5)

    income_stocks = []
    try:
        for index in range(1, 100, 1):
            # ウィンドウのハンドルを取得
            original_window = driver.current_window_handle

            row_element = driver.find_element(
                By.XPATH, f'//*[@id="gridview-1176-body"]/tr[{index}]/td[{2}]/div/a'
            )
            income_id = row_element.text
            row_element.click()

            # 新しいウィンドウが開くのを待つ
            time.sleep(2)  # 必要に応じて待機時間を調整

            all_windows = driver.window_handles

            # 新しく開いたウィンドウにフォーカスを当てる
            for window in all_windows:
                if window != original_window:
                    driver.switch_to.window(window)
                    break

            date_ele = driver.find_element(By.ID, "inbound_schd_dttm")
            date = date_ele.get_attribute("value")

            item_list = []
            try:
                for j in range(1, 100, 1):
                    jan_ele = driver.find_element(By.ID, f"common_product_cd{j}")
                    jan = jan_ele.text

                    amount_ele = driver.find_element(By.ID, f"qty{j}")
                    amount = amount_ele.get_attribute("value")
                    item_list.append({"jan_code": jan, "amount": amount})
            except:
                print(f"{income_id}: {j}商品の入荷情報")

            income_stocks.append(
                {"income_id": income_id, "income_at": date, "item_list": item_list}
            )

            time.sleep(1)

            driver.switch_to.window(original_window)
            wait = WebDriverWait(driver, 60)
    except:
        print(f"{index}まで成功")

    df = pd.json_normalize(
        income_stocks, record_path="item_list", meta=["income_at", "income_id"]
    )
    df["income_at"] = pd.to_datetime(df["income_at"])
    df["income_id"] = df["income_id"].astype(int)
    df["jan_code"] = df["jan_code"].astype(str)
    df["amount"] = df["amount"].str.replace(",", "").astype(int)
    df["partition_date"] = today

    bq_client = bigquery.Client(project="doctor-ilcsi")

    bigquery_job = bq_client.load_table_from_dataframe(
        df,
        "doctor-ilcsi.dl_crossmall.income_info",
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="partition_date"
            ),
        ),
    ).result()
