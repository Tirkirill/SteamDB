from selenium import webdriver
from selenium.webdriver.common.by import By

def get_tags_info_of_app(app_id: int, seen_tags:set, language:str='default') -> list[list[str, int]]:
    """
    Заходит на страницу приложения и считывает имена меток
    :param app_id: id приложения
    :param seen_tags: id меток, которые нужно пропустить
    :param language: Язык страницы (и соответственно меток)
    :return: Список[Список[Имя метки, id Метки]]
    """
    driver = webdriver.Chrome()
    s_app_id = str(app_id)
    url = "https://store.steampowered.com/app/" + s_app_id + ("" if language == "default" else "?l=" + language)
    driver.get(url)
    data = []
    try:
        elem = driver.find_element(By.CLASS_NAME, "add_button")
        elem.click()
        elems = driver.find_elements(By.CLASS_NAME, "app_tag_control")
        for elem in elems:
            tag_id = int(elem.get_attribute("data-tagid"))
            if tag_id in seen_tags:
                continue
            tag_name = elem.find_element(By.CLASS_NAME, "app_tag").text.replace("'", "")
            data.append([tag_name, tag_id])
            seen_tags.add(tag_id)
    except Exception as e:
        return None

    return data