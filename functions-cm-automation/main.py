import os
import shutil
import stat
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

# ChromeのWebDriverライブラリをインポート
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait


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
    username.send_keys("tajimura0505@gmail.com")  # ユーザ名を入力
    password.send_keys("Organicorganic11")  # パスワードを入力
    driver.find_element(
        By.NAME, "commit"
    ).click()  # ログインボタンをクリック（name属性が'login'の場合）

    driver.get(
        "https://www.crossmall.jp/rfc_batch/?mId=&umId=0"
    )  # 特定のページのURLを指定

    update_input_element = driver.find_element(
        By.XPATH, f'//*[@id="list"]/div[2]/table/tbody/tr[6]/td[5]/input'
    )
    # aタグをクリックする
    update_input_element.click()

    yes_button = driver.find_element(By.ID, "button-1006-btnIconEl")
    yes_button.click()

    button2 = driver.find_element(By.ID, "inv_flg")
    button2.click()

    button3 = driver.find_element(By.ID, "upd_stock_flg")
    button3.click()

    button4 = driver.find_element(By.ID, "button-1135-btnIconEl")
    button4.click()

    time.sleep(10)

    update_rsl()

    driver.quit()

    return "200"


def update_rsl():

    today = date.today()

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
        for index in range(1, 100, 1):
            row_element = driver.find_element(
                By.XPATH, f'//*[@id="gridview-1175-body"]/tr[{index}]/td[{9}]/div'
            )
            jan_code = row_element.text

            row_element = driver.find_element(
                By.XPATH, f'//*[@id="gridview-1175-body"]/tr[{index}]/td[{11}]/div/a'
            )
            stock = row_element.text

            stocks.append({"jan_code": jan_code, "stock": stock})
    except:
        print(f"{index}まで成功")

    df_stock = pd.DataFrame(stocks)
    df_stock["jan_code"] = df_stock["jan_code"].astype(str)
    df_stock["stock"] = df_stock["stock"].astype(int)
    df_stock["partition_date"] = today

    bq_client = bigquery.Client(project="doctor-ilcsi")

    bigquery_job = bq_client.load_table_from_dataframe(
        df_stock,
        "doctor-ilcsi.dl_crossmall.rsl_zaiko",
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="partition_date"
            ),
        ),
    ).result()
