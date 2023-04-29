from script_funcs import save_app_list, load_app_list_sql, load_genres_categories_prices, clear_tables
from settings import LOG_FILENAME, LOGGING_IS_REQUIRED
import logging

if __name__ == '__main__':
    if LOGGING_IS_REQUIRED:
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        handler = logging.FileHandler(LOG_FILENAME, 'a', 'utf-8')
        root_logger.addHandler(handler)

    script_scenario = [
        #"clear_tables",
        "load_details"
    ]

    script_params = [
        #[["apps_prices", "apps_genres", "apps_categories"]],
        [100, True]
    ]

    action_func = {
        "save_app_list":    save_app_list,
        "load_apps":        load_app_list_sql,
        "clear_tables":     clear_tables,
        "load_details":     load_genres_categories_prices
    }

    for action, params in zip(script_scenario, script_params):
        action_func[action](*params)









