from settings import LOGGING_IS_REQUIRED, DB_CONFIGURATION_FILENAME
import json
import requests
import logging
import psycopg2

def get_db_params() -> dict:
    with open(DB_CONFIGURATION_FILENAME, 'r') as f:
        return json.load(f)

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
            game_data = {"no_data": True}
            if LOGGING_IS_REQUIRED:
                logging.warning("No details for id: " + s_id)
    else:
        if LOGGING_IS_REQUIRED:
            logging.warning("No details for id: " + s_id + ". Status code = " + str(res.status_code))

    return game_data


def get_loaded_details_ids() -> set:
    res_set = set()
    db_params = get_db_params()
    try:
        conn = psycopg2.connect(**db_params)
    except Exception as e:
        if LOGGING_IS_REQUIRED: logging.error("SQLError", exc_info=True)
        raise e

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
        WHERE apps.no_data = True"""
        )
        records = cursor.fetchall()
        for row in records:
            res_set.add(row[0])
    except Exception as e:
        if LOGGING_IS_REQUIRED:
            logging.error("SQLError", exc_info=True)
        cursor.close()
        conn.close()
        raise e

    return res_set