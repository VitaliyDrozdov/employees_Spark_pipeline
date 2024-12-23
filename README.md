# ETL HTTPS - Spark - HDFS

# Описание проекта
ETL pipeline. Включает в себя загрузку по ссылке с корпоративного портала файлов Excel из Jira с задолжниками по списанию времени. Файлы загружаются на локальный диск во временную директорию. После чего обабатываются Spark, создается новый файл с аггрегированными данными. Далее файл загружается в HDFS.
## Используемый стек

- Python
- Selenium
- BeautifulSoup
- requests
- Spark
- HDFS


## Требования

1. **Python 3.12**
2. **Spark**
3. **Java**
4. **Docker**


## Запуск

1. Устанавливаем все зависимости из раздела "Требования".

2. Устанавливаем зависимости из requirements.txt

3. Создаём `.env` файл в корневой директории проекта и заполняем его по
образцу `.env.example`

4. Запускаем скрипт

   ```
   pythnon employees_agg.py
   
   ```


