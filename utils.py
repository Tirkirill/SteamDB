from settings import LOGGING_IS_REQUIRED, DB_CONFIGURATION_FILENAME, BINS_FILENAME
import json
import requests

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
            if LOGGING_IS_REQUIRED:
                logging.warning("No details for id: " + s_id)
    else:
        if LOGGING_IS_REQUIRED:
            logging.warning("No details for id: " + s_id + ". Status code = " + str(res.status_code))

    return game_data


def get_loaded_ids() -> set:
    res_set = set()
    try:
        with open(BINS_FILENAME, 'r') as f:
            for line in f:
                res_set.add(int(line))
    except:
        pass

    return res_set

def save_loaded_ids(ids: list) -> None:
    with open(BINS_FILENAME, 'a') as f:
        for id in map(str, ids):
            f.write(id + "\n")


def clear_bins_file() -> None:
    with open(BINS_FILENAME, 'w') as f:
        f.write("")