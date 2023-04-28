import json
import requests
import psycopg2
from settings import DB_CONFIGURATION_FILENAME, APP_LIST_FILENAME, LOGGING_IS_REQUIRED
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

def clear_tables(tables: list) -> None:
    """
    Удаляет данные из таблиц
    :param tables: список строк, строка - название таблицы
    """
    db_params = get_db_params()
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    try:
        for table in tables:
            cursor.execute("DELETE FROM " +  table)
        conn.commit()
    except Exception as e:
        conn.rollback
        raise e
    finally:
        if conn:
            cursor.close()
            conn.close()

def copy_required_data(data: dict, keys: list, raise_exception=False) -> dict:
    """
    Возвращает словарь с необходимыми данными
    :param data: Словарь, из которого нужно извлчечь только данные и ключами keys
    :param keys: Необходимые ключи
    :return: Словарь
    """
    req_data = {}
    for key in keys:
        if key not in data:
            if raise_exception:
                raise KeyError()
        else:
            req_data[key] = data[key]
    return req_data

def get_details(id: int) -> dict:
    s_id = str(id)
    res = requests.get("https://store.steampowered.com/api/appdetails?appids=" + s_id + "&cc=ru")
    game_data = None
    if res.status_code == 200:
        res_json = res.json()[s_id]
        if "success" in res_json and res_json["success"] and "data" in res_json:
            res_json = res_json["data"]
            game_data = copy_required_data(res_json, ["genres", "categories"])
            game_data["price"] = None
            if res_json["is_free"]:
                game_data["price"] = 0
            else:
                if "price_overview" in res_json:
                    game_data["price"] = res_json["price_overview"]["final"] // 100
                else:
                    if LOGGING_IS_REQUIRED:
                        logging.warning("No price for id: " + s_id)
        else:
            if LOGGING_IS_REQUIRED:
                logging.warning("No details for id: " + s_id)
    else:
        if LOGGING_IS_REQUIRED:
            logging.warning("No details for id: " + s_id + ". Status code = " + str(res.status_code))

    return game_data


def load_details(track_progress=True) -> None:
    db_params = get_db_params()
    try:
        conn = psycopg2.connect(**db_params)
    except Exception as e:
        if LOGGING_IS_REQUIRED: logging.error("SQLError", exc_info=True)
        return

    cursor = conn.cursor()

    seen_genres = set()
    seen_categories = set()

    try:
        cursor.execute(""" SELECT id from genres """)
    except Exception as e:
        print(e)
        if LOGGING_IS_REQUIRED:
            logging.error("SQLError", exc_info=True)
        cursor.close()
        conn.close()
        return

    records = cursor.fetchall()
    for row in records:
        seen_genres.add(row[0])

    print("Выбрали жанры")

    try:
        cursor.execute(""" SELECT id from categories """)
    except Exception as e:
        print(e)
        if LOGGING_IS_REQUIRED:
            logging.error("SQLError", exc_info=True)
        cursor.close()
        conn.close()
        return

    records = cursor.fetchall()
    for row in records:
        seen_categories.add(row[0])

    print("Выбрали категории")

    try:
        cursor.execute(""" SELECT id from apps """)
    except Exception as e:
        if LOGGING_IS_REQUIRED:
            logging.error("SQLError", exc_info=True)
        cursor.close()
        conn.close()
        return

    records = cursor.fetchall()
    data_prices = []
    data_genres = []
    data_categories = []

    data_new_categories = []
    data_new_genres = []

    if track_progress:
        bar = IncrementalBar('Countdown', max=len(records))

    for row in records:
        id = row[0]

        details = get_details(id)

        if details is not None:
            if "categories" in details:
                for category_row in details["categories"]:
                    category_id = category_row["id"]
                    if category_id not in seen_categories:
                        seen_categories.add(category_id)
                        category_name = category_row["description"]
                        data_new_categories.append([category_id, category_name])

                    data_categories.append([id, category_id])

            if "genres" in details:
                for genre_row in details["genres"]:
                    genre_id = genre_row["id"]
                    if genre_id not in seen_genres:
                        seen_genres.add(genre_id)
                        genre_name = genre_row["description"]
                        data_new_genres.append([genre_id, genre_name])

                    data_genres.append([id, genre_id])

            if "price" in details:
                data_prices.append([id, details["price"]])

        if track_progress:
            bar.next()

    if track_progress:
        bar.finish()

    if len(data_new_genres) > 0:
        try:
            cursor.executemany(""" INSERT INTO genres (id, name) VALUES (%s, %s) """, data_new_genres)
        except Exception as e:
            if LOGGING_IS_REQUIRED:
                logging.error("SQLError", exc_info=True)
            raise e

    if len(data_new_categories) > 0:
        try:
            cursor.executemany(""" INSERT INTO categories (id, name) VALUES (%s, %s) """, data_new_categories)
        except Exception as e:
            if LOGGING_IS_REQUIRED:
                logging.error("SQLError", exc_info=True)
            raise e

    try:
        try:
            cursor.executemany(""" INSERT INTO apps_categories (app_id, category_id) VALUES (%s, %s) """,
                               data_categories)
        except Exception as e:
            if LOGGING_IS_REQUIRED:
                logging.error("SQLError", exc_info=True)
            raise e

        try:
            cursor.executemany(""" INSERT INTO apps_genres (app_id, genre_id) VALUES (%s, %s) """,
                               data_genres)
        except Exception as e:
            if LOGGING_IS_REQUIRED:
                logging.error("SQLError", exc_info=True)
            raise e

        try:
            cursor.executemany(""" INSERT INTO apps_prices (app_id, price) VALUES (%s, %s) """, data_prices)
        except Exception as e:
            if LOGGING_IS_REQUIRED:
                logging.error("SQLError", exc_info=True)
            raise e

        conn.commit()

    except Exception as e:
        print(e)
        conn.rollback()

    finally:
        if conn:
            cursor.close()
            conn.close()

