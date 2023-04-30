import json
import requests
import psycopg2
import logging
from progress.bar import IncrementalBar
import time

from settings import APP_LIST_FILENAME, LOGGING_IS_REQUIRED
from utils import get_details, copy_required_data, get_db_params, get_loaded_details_ids, get_seen_objects, \
    get_loaded_tags_id, get_apps_ids, get_tags_data, get_steam_client

def save_app_list() -> None:
    """
    Записывает список приложений (список словарей с ключами appid и name) в файл
    """

    print("Записывание списка приложений -- Начало")

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

    print("Записывание списка приложений -- Окончание")

def load_app_list_sql() -> None:
    """
    Вставляет данные (id и название) о приложениях в sql-таблицу apps
    """

    print("Загрузка списка приложений -- Начало")

    db_params = get_db_params()

    with open(APP_LIST_FILENAME, 'r') as f:
        data = [(_dict["appid"], _dict["name"][:50]) for _dict in json.load(f)]

    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    cursor.executemany(""" INSERT INTO apps (id, name) VALUES (%s, %s) """, data)
    conn.commit()
    cursor.close()
    conn.close()

    print("Загрузка списка приложений -- Окончание")

def load_genres_categories_prices(bin=100, track_bar=True) -> None:
    """
    Выбирает из таблицы все приложения и получает по ним категории, жанры и цену.
    Вставка данных в таблицу происходит пачками (размер: bin)
    :param bin: размер пачки
    :param track_bar: если параметр = True, то в консоли будет отображаться прогресс полоской загрузки
    False - просто выводом
    """
    print("Загрузка жанров, категорий и цен -- Начало")

    db_params = get_db_params()
    try:
        conn = psycopg2.connect(**db_params)
    except Exception as e:
        if LOGGING_IS_REQUIRED:
            logging.error("SQLError", exc_info=True)
        return

    cursor = conn.cursor()

    seen_genres = get_seen_objects("genres", conn, cursor)
    seen_categories = get_seen_objects("categories", conn, cursor)
    seen_id = get_loaded_details_ids(conn, cursor)
    records = get_apps_ids(conn, cursor)

    records_len = len(records)

    if track_bar:
        bar = IncrementalBar('Countdown', max=len(records))

    finished = False

    if not track_bar:
        print("Начало загрузки")

    while not finished:
        data_prices = []
        data_genres = []
        data_categories = []

        data_new_categories = []
        data_new_genres = []

        no_data_ids = []

        i = 0

        for records_i, id in enumerate(records):
            if i == bin:
                if not track_bar:
                    print("Начало записи до " + str(records_i) + " из " + str(records_len))
                break

            if id in seen_id:
                continue

            seen_id.add(id)

            details = get_details(id)
            time.sleep(0.75)

            if details is not None:
                if "no_data" in details and details["no_data"]:
                    no_data_ids.append(id)

                if "categories" in details:
                    for category_row in details["categories"]:
                        category_id = int(category_row["id"])
                        if category_id not in seen_categories:
                            seen_categories.add(category_id)
                            category_name = category_row["description"]
                            data_new_categories.append([category_id, category_name])

                        data_categories.append([id, category_id])

                if "genres" in details:
                    for genre_row in details["genres"]:
                        genre_id = int(genre_row["id"])
                        if genre_id not in seen_genres:
                            seen_genres.add(genre_id)
                            genre_name = genre_row["description"]
                            data_new_genres.append([genre_id, genre_name])

                        data_genres.append([id, genre_id])

                if "price" in details:
                    data_prices.append([id, details["price"]])

            if track_bar:
                bar.goto(records_i+1)

            i += 1
            if records_i == records_len - 1:
                finished = True

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

        if len(no_data_ids) > 0:
            try:
                update_query = 'UPDATE apps SET no_data_details = True WHERE id IN {0}'\
                    .format(no_data_ids).replace("[", "(").replace("]", ")")
                cursor.execute(update_query)
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
            conn.rollback()
            break

        if not track_bar:
            print("Запись прошла успешно")

        time.sleep(5)

    if conn:
        cursor.close()
        conn.close()

    if track_bar:
        bar.finish()

    print("Загрузка жанров, категорий и цен -- Окончание")

def load_store_tags(bin=100, max_tag_order=None,track_bar=True) -> None:
    """
    :param bin: размер пачки
    :param track_bar: если параметр = True, то в консоли будет отображаться прогресс полоской загрузки
    False - просто выводом
    """

    print("Загрузка меток -- Начало")

    db_params = get_db_params()
    try:
        conn = psycopg2.connect(**db_params)
    except Exception as e:
        if LOGGING_IS_REQUIRED:
            logging.error("SQLError", exc_info=True)
        return

    cursor = conn.cursor()

    seen_tags = get_seen_objects("store_tags", conn, cursor)
    seen_id = get_loaded_tags_id(conn, cursor)
    records = get_apps_ids(conn, cursor)

    records_len = len(records)

    if track_bar:
        bar = IncrementalBar('Countdown', max=len(records))

    finished = False

    if not track_bar:
        print("Начало загрузки")

    # Если включена 2-ух факторная аутентификация, то придется ввести код с телефона/почты
    client = get_steam_client()
    while not finished:
        i = 0
        new_ids = set()
        for records_i, row in enumerate(records):
            if i == bin:
                break

            id = row

            if id not in seen_id:
                seen_id.add(id)
                new_ids.add(id)

                if track_bar:
                    bar.goto(records_i+1)

                i += 1

            if records_i == records_len - 1:
                finished = True

        new_tags = set()
        no_tags_ids = set()
        if len(new_ids) == 0:
            break

        tags_data = get_tags_data(client, new_ids, seen_tags, new_tags, no_tags_ids, max_tag_order=max_tag_order)
        data_new_tags = [(tag, "") for tag in new_tags]

        print("Начало записи меток до " + str(records_i+1) + " из " + str(records_len))
        if len(data_new_tags) > 0:
            try:
                cursor.executemany(""" INSERT INTO store_tags (id, name) VALUES (%s, %s) """, data_new_tags)
            except Exception as e:
                if LOGGING_IS_REQUIRED:
                    logging.error("SQLError", exc_info=True)
                raise e

        if len(no_tags_ids) > 0:
            try:
                update_query = 'UPDATE apps SET no_data_tags = True WHERE id IN {0}' \
                    .format(no_tags_ids).replace("{", "(").replace("}", ")")
                cursor.execute(update_query)
            except Exception as e:
                if LOGGING_IS_REQUIRED:
                    logging.error("SQLError", exc_info=True)
                raise e

        try:
            try:
                cursor.executemany(""" INSERT INTO apps_store_tags (app_id, tag_id, tag_order) VALUES (%s, %s, %s) """,
                                   tags_data)
            except Exception as e:
                if LOGGING_IS_REQUIRED:
                    logging.error("SQLError", exc_info=True)
                raise e

            conn.commit()

        except Exception as e:
            conn.rollback()
            break

        if not track_bar:
            print("Запись прошла успешно")

        time.sleep(10)

    if conn:
        cursor.close()
        conn.close()

    if track_bar:
        bar.finish()

    print("Загрузка меток -- Окончание")

def clear_tables(tables: list) -> None:
    """
    Удаляет данные из таблиц
    :param tables: список строк, строка - название таблицы
    """

    print("Очищение таблиц -- Начало")

    db_params = get_db_params()
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    err = None
    try:
        for table in tables:
            cursor.execute("DELETE FROM " + table)
        conn.commit()
        print("Таблица " + table + " очищена")
    except Exception as e:
        conn.rollback()
        err = e
    finally:
        if conn:
            cursor.close()
            conn.close()
        if err:
            raise err

    print("Очищение таблиц -- Окочание")

