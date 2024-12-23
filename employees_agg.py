# https://googlechromelabs.github.io/chrome-for-testing/known-good-versions-with-downloads.json
import os
import time
import re
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, regexp_extract, lit

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from dotenv import load_dotenv

load_dotenv()


spark = (
    SparkSession.builder.appName("Employees_data")
    .config("spark.master", "local[2]")
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.5")
    .getOrCreate()
)
base_url = os.getenv("BASE_URL")
login_url = os.getenv("LOGIN_URL")
file_mask = "Список должников ноябрь 2024.xlsx"
local_dir = "./tmp"
output_file = "./employees_result/employees.csv"
hdfs_path = os.getenv("HDFS_PATH", "/data/employees/employees.csv")


os.makedirs(local_dir, exist_ok=True)


def selenium_driver_setup():
    options = Options()
    # Далее используется Chrome
    options.add_argument(
        "--headless"
    )  # запуск браузера без графического режима
    options.add_argument("--disable-gpu")  # без gpu
    options.add_argument("--no-sandbox")  # без режима sandbox
    driver_path = os.getenv("DRIVER_PATH")
    service = Service(driver_path) if driver_path else None
    driver = webdriver.Chrome(service=service, options=options)
    return driver


def login(driver, login_url, username=None, password=None):
    driver.get(login_url)
    username = driver.find_element(By.ID, "os_username")
    password = driver.find_element(By.ID, "os_password")
    username.send_keys(os.getenv("NF_USERNAME"))
    password.send_keys(os.getenv("NF_PASSWORD"))
    time.sleep(2)
    password.send_keys(Keys.RETURN)


def download_files(
    driver,
    file_mask,
    base_url=None,
):
    downloaded_files = []
    driver.get(base_url)
    WebDriverWait(driver=driver, timeout=10).until(
        EC.presence_of_element_located((By.CLASS_NAME, "filename"))
    )
    coookies = driver.get_cookies()
    time.sleep(2)
    session = requests.Session()
    for cookie in coookies:
        session.cookies.set(cookie["name"], cookie["value"])
    response = session.get(base_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    links = soup.find_all("a", class_="filename")
    for link in links:
        file_name = link.get("data-filename", "")
        if re.match(file_mask, file_name):
            href = link.get("href", "")
            print(f"href: {href}")
            cleaned_base_url = re.sub(
                r"pages/viewpage\.action\?pageId=\d+", "", base_url
            )
            file_url = (
                cleaned_base_url + href if href.startswith("/") else href
            )
            local_path = os.path.join(local_dir, file_name)
            with session.get(file_url, stream=True) as r:
                r.raise_for_status()
                with open(local_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            downloaded_files.append(local_path)
            print(f"Файл загружен: {local_path}")
    return downloaded_files


def main(file_path):
    df = (
        spark.read.format("com.crealytics.spark.excel")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("dataAddress", "'jira_dolg'!A4")
        .load(file_path)
    )

    file_name = os.path.basename(file_path)
    month_year_pattern = r"(янв|фев|март|апр|май|июнь|июль|авг|сент|окт|нояб|дек)[а-я]*\s(\d{4})"
    month_year = re.search(month_year_pattern, file_name).group(0)
    df = df.withColumn("Период", lit(month_year))
    return df


if __name__ == "__main__":
    driver = selenium_driver_setup()
    login(driver=driver, login_url=login_url)
    files = download_files(
        driver=driver, file_mask=file_mask, base_url=os.getenv("BASE_URL")
    )
    driver.quit()
    dataframes = [main(file) for file in files]

    combined_df = dataframes[0]
    for df in dataframes[1:]:
        combined_df = combined_df.union(df)

    res_df = combined_df.groupBy("Филиал сотрудника").agg(
        sum(
            "Плановое количество md согласно утвержденному графику работы"
        ).alias("sum_planned_md"),
        sum("Списанное в Jira количество md").alias("sum_md"),
        avg("Задолженность по списаниям, md").alias("sum_left_md"),
    )
    res_df.coalesce(1).write.option("header", "true").mode("overwrite").csv(
        output_file, header=True
    )
    spark.stop()
