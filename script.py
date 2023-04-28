from script_funcs import save_app_list, load_app_list_sql, load_details, clear_tables
from utils import  clear_bins_file
from settings import LOG_FILENAME, LOGGING_IS_REQUIRED
import logging

if __name__ == '__main__':
    if LOGGING_IS_REQUIRED:
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        handler = logging.FileHandler(LOG_FILENAME, 'w', 'utf-8')
        root_logger.addHandler(handler)

    script_scenario = [
        #"clear_bins_file",
        #"clear_tables",
        "load_details"
    ]

    script_params = [
        #[],
        #[["apps_prices", "apps_genres", "apps_categories"]],
        [100]
    ]

    action_func = {
        "save_app_list":    save_app_list,
        "load_apps":        load_app_list_sql,
        "clear_tables":     clear_tables,
        "load_details":     load_details,
        "clear_bins_file":  clear_bins_file
    }

    for action, params in zip(script_scenario, script_params):
        action_func[action](*params)









