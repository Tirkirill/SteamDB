import json
import requests
import psycopg2
from settings import DB_CONFIGURATION_FILENAME, APP_LIST_FILENAME

def load_app_list_sql() -> None:
    """
    Вставляет данные (id и название) о приложениях в sql-таблицу apps
    """

    with open(DB_CONFIGURATION_FILENAME, 'r') as f:
        db_params = json.load(f)

    with open(APP_LIST_FILENAME, 'r') as f:
        data = [(_dict["appid"], _dict["name"][:50]) for _dict in json.load(f)]

    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    cursor.executemany(""" INSERT INTO apps (id, name) VALUES (%s, %s) """, data)
    conn.commit()
    conn.close()

load_app_list_sql()







