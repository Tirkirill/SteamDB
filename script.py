from utils import save_app_list, load_app_list_sql, clear_apps_table

if __name__ == '__main__':
    script_scenario = [
        "load_apps"
    ]

    for action in script_scenario:
        if action == "save_app_list":
            save_app_list()
        if action == "load_apps":
            load_app_list_sql()
        if action == "clear_apps":
            clear_apps_table()









