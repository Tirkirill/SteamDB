import json
import requests
import psycopg2
from settings import DB_CONFIGURATION_FILENAME, APP_LIST_FILENAME, NEED_LOGGING
import logging
from progress.bar import IncrementalBar

def get_db_params() -> dict:
    with open(DB_CONFIGURATION_FILENAME, 'r') as f:
        return json.load(f)

def save_app_list() -> None:
    """
    Записывает список приложений (список словарей с ключами appid и name) в файл
    """
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

    db_params = get_db_params()

    with open(APP_LIST_FILENAME, 'r') as f:
        data = [(_dict["appid"], _dict["name"][:50]) for _dict in json.load(f)]

    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    cursor.executemany(""" INSERT INTO apps (id, name) VALUES (%s, %s) """, data)
    conn.commit()
    cursor.close()
    conn.close()

def clear_apps_table() -> None:
    db_params = get_db_params()
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM apps")
    conn.commit()
    cursor.close()
    conn.close()

def get_price(id: int) -> int:
    """
    Возвращает цену товара с помощью отдельного API
    Данный API поддерживает запрос только по одному id единовременно
    """
    s_id = str(id)
    price_req = requests.get("https://store.steampowered.com/api/appdetails?appids=" + s_id + "&cc=ru")
    price = None
    if price_req.status_code == 200:
        data = price_req.json()
        if data[s_id]["success"]:
            try:
                if "data" in data[s_id]:
                    game_data = data[s_id]["data"]
                    if game_data["is_free"]:
                        price = 0
                    else:
                        if "price_overview" in game_data:
                            price = data[s_id]["data"]["price_overview"]["final"] // 100
                        else:
                            if NEED_LOGGING:
                                logging.warning("No price for id: " + s_id)
                else:
                    if NEED_LOGGING:
                        logging.warning("No price for id: " + s_id)
            except Exception as e:
                if NEED_LOGGING:
                    logging.error("No price for id: " + s_id, exc_info=True)
        else:
            if NEED_LOGGING:
                logging.warning("No price for id: " + s_id)

    return price

def load_prices(track_progress=True) -> None:
    """
    Вставляет цены для всех id в таблице apps в app_prices
    """

    db_params = get_db_params()
    try:
        conn = psycopg2.connect(**db_params)
    except Exception as e:
        if NEED_LOGGING: logging.error("SQLError", exc_info=True)
        return

    cursor = conn.cursor()

    try:
        cursor.execute(""" SELECT id from apps """)
    except Exception as e:
        if NEED_LOGGING:
            logging.error("SQLError", exc_info=True)
        cursor.close()
        conn.close()
        return

    records = cursor.fetchall()
    data = []

    if track_progress:
        bar = IncrementalBar('Countdown', max=len(records))

    for row in records:
        id = row[0]
        price = get_price(id)
        if price is not None:
            data.append([id, price])

        if track_progress:
            bar.next()

    if track_progress:
        bar.finish()

    try:
        cursor.executemany(""" INSERT INTO app_prices (app_id, price) VALUES (%s, %s) """, data)
        conn.commit()
    except Exception as e:
        if NEED_LOGGING:
            logging.error("SQLError", exc_info=True)

    cursor.close()
    conn.close()

