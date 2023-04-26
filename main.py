
def copy_required_data(data: dict, keys: list) -> dict:
    """
    Возвращает словарь с необходимыми данными
    :param data: Словарь, из которого нужно извлчечь только данные и ключами keys
    :param keys: Необходимые ключи
    :return: Словарь
    """
    req_data = {}
    for key in keys:
        if key not in data:
            raise KeyError()
        req_data[key] = data[key]
    return req_data

def get_price(id: str) -> int:
    """
    Возвращает цену товара с помощью отдельного API
    Данный API поддерживает запрос только по одному id единовременно
    """
    price_req = requests.get("https://store.steampowered.com/api/appdetails?appids=" + id + "&cc=ru")
    if price_req.status_code == 200:
        data = price_req.json()
        if data[id]["success"]:
            price = data[id]["data"]["price_overview"]["final"] // 100

    return price

if __name__ == '__main__':
    from steam.client import SteamClient
    from steam.enums import EResult
    import json
    from TEST import game_ids
    API_KEY = "DB1835D805F384D43C5CA9CA54AAC2B7"
    LOGIN = "tirkirill"
    PASSWORD = "Xaoc1212"

    required_keys = [
        "gameid",
        "name",
        "primary_genre",
        "genres",
        "category",
        "store_tags",
        "steam_release_date",
        "type"
    ]

    client = SteamClient()
    res = client.cli_login(LOGIN, PASSWORD)
    if res == EResult.OK:
        names = ["Stardew Valley"]
        ids = [game_ids[name] for name in names]

        products_info = client.get_product_info(apps=ids)["apps"]
        need_full = False

        for id in ids:
            product = products_info[id]["common"]
            data = copy_required_data(product, required_keys)
            data["price"] = get_price(id)
            with open(id + ".json", 'w') as f:
                json.dump(data, f)

            if need_full:
                with open("full" + id + ".json", 'w') as f:
                    json.dump(products_info[id], f)

