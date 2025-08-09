import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Запрашивает список товаров с маркетплейса OZON через API
    
    Agrs: 
    last_id(str): Идентификатор последнего полученного товара. Для первого запуска используйте пустое значение
    client_id(str): Клиентский идентификатор для получения доступа к своему магазину на OZON
    seller_token(str): Токен аутентификации продавца
    
    Returns: 
    dict: Объект JSON где нас интересует значения 'result' и 'last_id'
    'result': поле которое содержит основную информацию о товаре
    'last_id': последний идентификатор товара для следующей страницы 

    Exceptions: 
    HTTPError: Возникает, если сервер вернул ошибку (код статуса >= 400) 
    KeyError: Возникает пре отсутствии поля 'result' """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Запрашивает артикулы всех товаров продавца с маркетплейса OZON через API, и формирует список артикулов
    
    
    Agrs: 
    client_id(str): Клиентский идентификатор для получения доступа к своему магазину на OZON
    seller_token(str):Токен аутентификации продавца  
    
    Returns: 
    List[str]: Список уникальных артикулов всех товаров продавца с маркетплейса OZON(offer_id)
     
    Exceptions:
    HTTPError: Возникает, если сервер вернул ошибку (код статуса >= 400) 
    
    Пример: 
    offer_ids = ['dfwd1254','zcsefr321'...] """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновляет массово цены товаров на маркетплейсе OZON у продавца
    
    Agrs: 
    prices(list): Список структурированных данных о ценах товаров 
    client_id(str):  Клиентский идентификатор для получения доступа к своему магазину на OZON
    seller_token(str): Токен аутентификации продавца  
    
    Returns: 
    dict: Ответ API в формате JSON, содержит информацию о статусе обновления.
    
    Exceptions:
    HTTPError: Возникает, если сервер вернул ошибку (код статуса >= 400)(неправильные учетные данные или сетевые проблемы) 
    
    Пример: 
    update_price(prices: list, client_id, seller_token){'result': 'Цены успешно обновлены.'} """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновляет массово остатки товаров на маркетплейсе OZON у продавца 
    
    Agrs: 
    stocks(list): Список структурированных данных о количестве каждого товара и их остатке 
    client_id(str):  Клиентский идентификатор для получения доступа к своему магазину на OZON
    seller_token(str): Токен аутентификации продавца
     
    Returns: 
    dict: Ответ API в формате JSON, содержит информацию о статусе обновления. 
    
    Exceptions:
    HTTPError: Возникает, если сервер вернул ошибку (код статуса >= 400)(неправильные учетные данные или сетевые проблемы) 
    
    Пример:
    update_stocks(stocks: list, client_id, seller_token) {'result': 'Остатки успешно обновлены.'}"""
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачивает файл ostatki с сайта timeworld.ru, извлекает содержимое архива, читает 
    файл Excel с 18 строки, преобразует данные в словарь и удаляет Excel файл после обработки
    
    Returns:
    dict: список словарей, где каждый словарь соответсвует одной записи из файла.
    
    Exceptions:
    requests.RequestException: Если возникла ошибка при получении данных с сайта. 
    FileNotFoundError: Если скачанный файл Excel не обнаружен после распаковки.
    
    Пример: 
    watch_remnants = [{'Код': 'ABCD1234', 'Название': 'Часы Casio...', 'Количество': '>10'}, ...]   """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids): 
    """Cоздает список запасов товаров на основе имеющихся остатков и загруженных 
    артикулов. Проходится по каждому товару из переменной `watch_remnants`, и выбирает 
    только те товары, где артикулы присутствуют в списке `offer_ids`. Вычисляются остатки
    по принципу: Если указано >10, выставляется остаток 100. Если указано как 1, 
    выставляется остаток 0. В остальных случаях берется точное значение. Для оставшихся 
    актикулов, не встречающихся в watch_remnants выставляется остаток 0. 
    
    Agrs: 
    watch_remnants(List[dict]): Список словарей, каждый из которых содержит информацию о конкретном товаре
    offer_ids(List[str]): Список артикулов товаров, зарегистрированных в Ozon.
    
    Returns:
    stocks(List[dict]):  Список словарей, каждый из которых содержит информацию о товаре и его остатке
     
    Exceptions: 
    TypeError: Если `watch_remnants` или `offer_ids` имеют неправильный тип данных.   
    ValueError: Если какая-либо запись в `watch_remnants` содержит недопустимые или неполные данные.
    
    Пример: 
    stocks =  [{'offer_id': 'Asdfre3', 'stock': 100}, {'offer_id': 'Xsdfwq3', 'stock': 0}]"""
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids): 
    """ Создает список цен товаров на основе данных из переменной `watch_remnants` и списке артикулов 
    из переменной `offer_ids`. Проходится по списку товаров(`watch_remnants`), фильтруя только те 
    товары, чей артикул указан в переменой (`offer_ids`). Создается переменная (`price`) которая 
    содержит информацию о цене, валюте. Переводит оригинальную цену в нужный формат с функции 
    (`price_conversion`) 
    
    Args: 
    watch_remnants(List[dict]):  Список словарей, каждый из которых содержит информацию о конкретном товаре
    offer_ids(List[str]): Список артикулов товаров, зарегистрированных в Ozon.
    
    Returns:
    prices(List[dict]): Список словарей, каждый из которых содержит информацию о цене товара 

    Exceptions: 
    TypeError: Если `watch_remnants` или `offer_ids` имеют неправильный тип данных.   
    ValueError: Если какая-либо запись в `watch_remnants` содержит недопустимые или неполные данные. 
    KeyError: Появится, если в словаре из `watch_remnants` отсутствуют обязательные поля ('Код' или 'Цена')
    
    Пример: 
    prices = [{'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB', 'offer_id': 'Asdgr3', 'old_price': '0', 'price': '5990'}] """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразование строки цены в числовой формат избавляясь от ненужных символов,отбрасывая значения после точки. 
    
    Args: 
    (price: str): Вводное значение цены
    
    Returns: 
    (str): Отформатированная цена  
    
    Пример правильной работы: 5'990.00 руб. -> 5990 
    Пример не правильной работы: 5'990.60 руб. -> 5990
    Пример не правильной работы: 5'990,60 руб. -> 599060 """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделяет список на фрагменты по `n` элементов. 
    
    Args: 
    lst(list): Исходный список подлежащий разделению 
    n(int): Колличество элементов в группе 
    
    Returns: 
    list: Список который имеет длину (`n`), за исключением последнего фрагмента, который может быть короче
    
    Exceptions: 
    TypeError: если lst не является списком, (`n`) должно быть положительным числом
    
    Пример: 
    list([1,2,3,4,5,6],[1,2,3,4,5,6]...)  """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token): 
    """ Загружает цены товаров  путем обновления ценовых предложений. 
    Создает цены на основе полученных остатков и обновляет существующие цены по 1000 записей.

    Args: 
    watch_remnants(List[dict]): Список словарей, каждый из которых содержит информацию о конкретном товаре 
    client_id(str): Клиентский идентификатор для получения доступа к своему магазину на OZON
    seller_token(str): Токен аутентификации продавца
     
    Returns: 
    list: Список созданных цен 
    
    Exceptions: 
    HTTPError: Возникает, если сервер вернул ошибку (код статуса >= 400) 
    TypeError: Если `watch_remnants` или `offer_ids` имеют неправильный тип данных."""
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token): 
    """ Загружает остатки товаров. Формирует остатки на основе текущих запасов товара
    и обновляет эти остатки. Создаются два списка: (`not_empty`) список объектов, где запас 
    не равен нулю. (`stocks`) полный список всех загруженных объектов. 

    Args: 
    watch_remnants(List[dict]): Список словарей, каждый из которых содержит информацию о конкретном товаре 
    client_id(str): Клиентский идентификатор для получения доступа к своему магазину на OZON
    seller_token(str): Токен аутентификации продавца
    
    Returns: 
    not_empty(list): Отфильтрованный список остатков, где колличество товара не равно нулю 
    stocks(list): Полный список всех загруженных объектов
    
    Exceptions: 
    HTTPError: Возникает, если сервер вернул ошибку (код статуса >= 400) 
    TypeError: Если `watch_remnants` или `offer_ids` имеют неправильный тип данных."""
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main(): 
    """ Основная точка входа программы для синхронизации остатков и цен товаров. 
    Эта функция управляет процессом полного обновления данных на маркетплейсе OZON:  
    1. Получает идентификационные данные продавца. 
    2. Запрашивает идентификаторы предложений (offer_ids). 
    3. Скачивает текущие остатки товаров (`download_stock()`). 
    4. Формирует структуры остатков (`create_stocks()`) и цен (`create_prices()`). 
    5. Обновляет остатки и цены  партиями. Процесс выполняется поэтапно,

    Returns: 
    None: Функция не возвращает никакого значения. 

    Exceptions:
    Все возможные ошибки отображаются в консоли с соответствующим сообщением. """
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
