import json
import requests
import psycopg2
from settings import DB_CONFIGURATION_FILENAME, APP_LIST_FILENAME


def save_app_list():
    res = requests.get("http://api.steampowered.com/ISteamApps/GetAppList/v0002/?format=json")
    if res.status_code == 200:
        data = res.json()["applist"]["apps"]

    unique_data = []
    seen = set()
    for _dict in data:
        appid = _dict["appid"]
        if appid not in seen:
            unique_data.append({"appid":appid, "name":_dict["name"]})
            seen.add(appid)

    with open(APP_LIST_FILENAME, 'w') as f:
        json.dump(unique_data, f)

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







