from settings import LOGGING_IS_REQUIRED, DB_CONFIGURATION_FILENAME, STEAM_GUARD_FILENAME
import json
import requests
import logging
import psycopg2
from steam.client import SteamClient
from steam.enums import EResult

def get_json_params(file_name:str) -> dict:
    with open(file_name, 'r') as f:
        return json.load(f)

def get_db_params() -> dict:
    return get_json_params(DB_CONFIGURATION_FILENAME)

def get_guard_params() -> dict:
    return get_json_params(STEAM_GUARD_FILENAME)

def get_steam_client():
    steam_params = get_guard_params()
    login = steam_params["LOGIN"]
    password = steam_params["PASSWORD"]
    client = SteamClient()
    res = client.cli_login(login, password)

    if res == EResult.OK:
        return client
    else:
        raise ConnectionAbortedError("Не удалось войти в аккаунт steam. Результат = ", str(res))

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

def get_details(id: int, max_attempts:int=5) -> dict:
    s_id = str(id)
    attempt_i = 0
    get_res = False
    while not get_res:
        try:
            res = requests.get("https://store.steampowered.com/api/appdetails?appids=" + s_id + "&cc=ru")
            get_res = True
        except Exception as e:
            attempt_i += 1
            print("Попытка обратиться к серверу : " + str(attempt_i))
            if attempt_i == max_attempts:
                raise e
    game_data = None
    if res.status_code == 200:
        try:
            res_json = res.json()
        except Exception as e:
            if LOGGING_IS_REQUIRED:
                logging.error("Не удалось расшифровать json id: " + s_id, exc_info=e)
            raise e
        res_json = res_json[s_id]
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
            game_data = {"no_data": True}
            if LOGGING_IS_REQUIRED:
                logging.warning("No details for id: " + s_id)
    else:
        if LOGGING_IS_REQUIRED:
            logging.warning("No details for id: " + s_id + ". Status code = " + str(res.status_code))

    if (("genres" not in game_data) or (game_data["genres"]) == [])\
            and (("categories" not in game_data) or (game_data["categories"]) == [])\
            and (("price" not in game_data) or (game_data["price"] is None)):
        game_data = {"no_data": True}

    return game_data

def get_tags_data(client, ids: set[int], seen_tags:set=None, new_tags:set=None, no_tags_ids:set=None,
                  max_tag_order:int=None):
    res = []
    products_info = client.get_product_info(apps=ids)["apps"]
    for id in ids:
        if id in products_info and "common" in products_info[id] and "store_tags" in products_info[id]["common"]:
            tags_info = products_info[id]["common"]["store_tags"]
            for tag_order, tag_id in tags_info.items():
                tag_order_int = int(tag_order)
                if max_tag_order is not None and max_tag_order < tag_order_int:
                    break
                tag_id_int = int(tag_id)
                if seen_tags is not None and new_tags is not None:
                    if tag_id_int not in seen_tags:
                        new_tags.add(tag_id_int)
                        seen_tags.add(tag_id_int)
                res.append((id, tag_id_int, tag_order_int))
        else:
            if LOGGING_IS_REQUIRED:
                logging.warning("No tags for " + str(id))
            if no_tags_ids is not None:
                no_tags_ids.add(id)
    return res

def get_loaded_details_ids(conn=None, cursor=None) -> set:
    """
    :param conn: Подключение
    :param cursor: Курсор
    :return: Множество id, для которых не нужно получать информацию по ценам, категория и жанрам
    """

    if not conn:
        db_params = get_db_params()
        conn = psycopg2.connect(**db_params)

    if not cursor:
        cursor = conn.cursor()

    try:
        cursor.execute(
        """SELECT DISTINCT apps_categories.app_id FROM apps_categories
        UNION
        SELECT DISTINCT apps_genres.app_id FROM apps_genres
        UNION
        SELECT DISTINCT apps_prices.app_id FROM apps_prices
        UNION
        SELECT apps.id FROM apps
        WHERE apps.no_data_details = True"""
        )
    except Exception as e:
        if conn:
            cursor.close()
            conn.close()
        raise e

    return set([row[0] for row in cursor.fetchall()])

def get_seen_objects(table_name: str, conn=None, cursor=None) -> set:
    """
    Возвращает id, которые уже записаны в таблицу
    :param table_name: Строка - название таблицы в postgres
    :param conn: Подключение
    :param cursor: Курсор
    :return: Множество id, которые уже записаны в таблицу
    """

    if not conn:
        db_params = get_db_params()
        conn = psycopg2.connect(**db_params)

    if not cursor:
        cursor = conn.cursor()

    try:
        cursor.execute(""" SELECT DISTINCT id from """ + table_name)
    except Exception as e:
        if conn:
            cursor.close()
            conn.close()
        raise e

    return set([row[0] for row in cursor.fetchall()])

def get_loaded_tags_id(conn=None, cursor=None) -> set:
    """
    :param conn: Подключение
    :param cursor: Курсор
    :return: Множество id, для которых не нужно получать информацию по меткам
    """
    if not conn:
        db_params = get_db_params()
        conn = psycopg2.connect(**db_params)

    if not cursor:
        cursor = conn.cursor()

    try:
        cursor.execute("""
        SELECT DISTINCT app_id FROM apps_store_tags
        UNION
        SELECT id FROM apps
        WHERE no_data_tags = True""")
    except Exception as e:
        if conn:
            cursor.close()
            conn.close()
        raise e

    return set([row[0] for row in cursor.fetchall()])

def get_apps_ids(conn=None, cursor=None) -> set:
    """
    :param conn: Подключение
    :param cursor: Курсор
    :return: Множество id приложений
    """
    if not conn:
        db_params = get_db_params()
        conn = psycopg2.connect(**db_params)

    if not cursor:
        cursor = conn.cursor()

    try:
        cursor.execute(""" SELECT id from apps """)
    except Exception as e:
        if conn:
            cursor.close()
            conn.close()
        raise e

    return set([row[0] for row in cursor.fetchall()])





    
