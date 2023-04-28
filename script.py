from utils import save_app_list, load_app_list_sql, clear_apps_table, load_details
from settings import LOG_FILENAME, LOGGING_IS_REQUIRED
import logging

if __name__ == '__main__':
    if LOGGING_IS_REQUIRED:
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        handler = logging.FileHandler(LOG_FILENAME, 'w', 'utf-8')
        root_logger.addHandler(handler)

    script_scenario = [
        "load_details"
    ]

    for action in script_scenario:
        if action == "save_app_list":
            save_app_list()
        if action == "load_apps":
            load_app_list_sql()
        if action == "clear_apps":
            clear_apps_table()
        if action == "load_details":
            load_details()









