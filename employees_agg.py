import os
import sys
import time
import re
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, lit

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from config import Config

config = Config()

spark = (
    SparkSession.builder.appName("Employees_data")
    .config("spark.master", "local[2]")
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.5")
    .getOrCreate()
)


def selenium_driver_setup():
    """Настройка драйвера Selenium."""
    options = Options()
    # Далее используется Chrome
    options.add_argument(
        "--headless"
    )  # запуск браузера без графического режима
    options.add_argument("--disable-gpu")  # без gpu
    options.add_argument("--no-sandbox")  # без режима sandbox
    service = Service(config.DRIVER_PATH) if config.DRIVER_PATH else None
    return webdriver.Chrome(service=service, options=options)


def soup_config(session, base_url):
    """Получение HTML контента через BeautifulSoup."""
    response = session.get(base_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    return soup


def login(driver, login_url):
    """Логин в систему через Selenium."""
    driver.get(login_url)
    username = driver.find_element(By.ID, "os_username")
    password = driver.find_element(By.ID, "os_password")
    username.send_keys(config.NF_USERNAME)
    password.send_keys(config.NF_PASSWORD)
    time.sleep(1.5)
    password.send_keys(Keys.RETURN)


def download_files(
    driver,
    file_mask,
    base_url,
):
    """Загрузка файлов с сайта через Selenium и requests."""
    downloaded_files = []
    driver.get(base_url)
    WebDriverWait(driver=driver, timeout=10).until(
        EC.presence_of_element_located((By.CLASS_NAME, "filename"))
    )
    session = requests.Session()
    coookies = driver.get_cookies()
    for cookie in coookies:
        session.cookies.set(cookie["name"], cookie["value"])
    soup = soup_config(session, base_url)
    links = soup.find_all("a", class_="filename")

    for link in links:
        file_name = link.get("data-filename", "")
        if re.match(file_mask, file_name):
            href = link.get("href", "")
            cleaned_base_url = re.sub(
                r"pages/viewpage\.action\?pageId=\d+", "", base_url
            )
            file_url = (
                cleaned_base_url + href if href.startswith("/") else href
            )
            local_path = os.path.join(config.LOCAL_DIR, file_name)
            with session.get(file_url, stream=True) as r:
                r.raise_for_status()
                with open(local_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            downloaded_files.append(local_path)
            print(f"Файл загружен: {local_path}")
    return downloaded_files


def read_excel_files(file_path):
    try:
        df = (
            spark.read.format("com.crealytics.spark.excel")
            .option("header", "true")
            .option("inferSchema", "true")
            .option("dataAddress", "'jira_dolg'!A4")
            .load(file_path)
        )

        file_name = os.path.basename(file_path)
        match = re.search(config.FILE_MASK, file_name).group(0)
        if not match:
            raise ValueError(
                f"Невозможно извлечь период из имени файла: {file_name}"
            )
        df = df.withColumn("Период", lit(match))
        return df
    except Exception as e:
        print(f"Ошибка при обработке файла {file_path}: {e}")
        return None


def align_columns(dataframes):
    """Приводит DataFrame к одинаковой структуре (одинаковые столбцы)."""
    all_columns = list(set(col for df in dataframes for col in df.columns))
    aligned_dfs = []
    for df in dataframes:
        missing_columns = set(all_columns) - set(df.columns)
        # Добавить отсутствующие столбцы с None
        for col_name in missing_columns:
            df = df.withColumn(col_name, lit(None))
        # Удалить лишние столбцы
        df = df.select([col for col in all_columns if col in df.columns])
        aligned_dfs.append(df)
    return aligned_dfs


def spark_transform(dataframes):
    dataframes = align_columns(dataframes)

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
        config.OUTPUT_FILE, header=True
    )


if __name__ == "__main__":
    os.makedirs(config.LOCAL_DIR, exist_ok=True)
    driver = selenium_driver_setup()
    try:
        login(driver=driver, login_url=config.LOGIN_URL)
        files = download_files(
            driver=driver,
            file_mask=config.FILE_MASK,
            base_url=config.BASE_URL,
        )
        if not files:
            print("Нет файлов.")
            sys.exit(-1)
    finally:
        driver.quit()
    dataframes = [read_excel_files(file) for file in files]
    dataframes = [df for df in dataframes if df is not None]
    spark_transform(dataframes)
    spark.stop()

    # TODO: Добавить загрузку в HDFS
    # csv_file = [
    #     os.path.join(output_file, f)
    #     for f in os.listdir(output_file)
    #     if f.endswith(".csv")
    # ][0]
    # with open(csv_file, "rb") as f:
    #     pass
