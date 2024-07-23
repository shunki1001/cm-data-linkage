import os
import shutil
import stat
import time
from pathlib import Path

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

    # button3 = driver.find_element(By.ID, "upd_stock_flg")
    # button3.click()

    button4 = driver.find_element(By.ID, "button-1135-btnIconEl")
    button4.click()

    time.sleep(10)
    driver.quit()

    return "200"
