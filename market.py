import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token): 
    """ Запрашивает список товаров с сайта Yandex.Market 
    
    Args: 
     page (str): Токен следующей страницы для пагинации
    (начальное значение должно быть пустым, если это первый запрос). 
    campaign_id(str): Идентификатор продавца 
    access_token(str): Токен аутентификации компании(продавца) 
    
    Returns: 
    dict: Словарь где нас интересует значения 'result' и 'page_token'
    'result': Поле которое содержит основную информацию о товаре
    'page_token': Последний идентификатор товара для следующей страницы 
    
    Exceptions: 
    HTTPError: Возникает, если сервер вернул ошибку (код статуса >= 400) 
    KeyError: Возникает пре отсутствии поля 'result' """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token): 
    """Обновляет массово остатки товаров  у продавца с сайта Yandex.Market
    
    Agrs: 
    stocks(list): Список структурированных данных о количестве каждого товара и их остатке 
    campaign_id(str): Идентификатор продавца
    access_token(str): Токен аутентификации компании(продавца)
     
    Returns: 
    dict: Словарь, содержит информацию о статусе обновления. 
    
    Exceptions:
    HTTPError: Возникает, если сервер вернул ошибку (код статуса >= 400)(неправильные учетные данные или сетевые проблемы) 
    
    Пример:
    update_stocks(stocks, campaign_id, access_token) {'result': 'Остатки успешно обновлены.'}"""
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token): 
    """Обновляет массово цены товаров у продавца с сайта Yandex.Market
    
    Agrs: 
    prices(list): Список структурированных данных о ценах товаров 
    campaign_id(str): Идентификатор продавца для получения доступа к своему магазину на сайте Yandex.Market
    access_token(str): Токен аутентификации компании(продавца) 
    
    Returns: 
    dict: Словарь содержит информацию о статусе обновления.
    
    Exceptions:
    HTTPError: Возникает, если сервер вернул ошибку (код статуса >= 400)(неправильные учетные данные или сетевые проблемы) 
    
    Пример: 
    update_price(prices, campaign_id, access_token){'result': 'Цены успешно обновлены.'} """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Запрашивает артикулы всех товаров продавца с сайта Yandex.Market через API, и формирует список артикулов
    
    
    Agrs: 
    campaign_id(str): Клиентский идентификатор для получения доступа к своему магазину на сайте Yandex.Market
    market_token(str):Токен аутентификации для авторизации в API Яндекс.Маркета.  
    
    Returns: 
    List[str]: Список уникальных артикулов (SKU) всех товаров компании(продавца)(offer_id)
     
    Exceptions:
    HTTPError: Возникает, если сервер вернул ошибку (код статуса >= 400) 
    
    Пример: 
    offer_ids = ['dfwd1254','zcsefr321'...] """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Cоздает список запасов товаров совместимая с требованием API Yandex.Market на основе 
    имеющихся остатков и загруженных артикулов. Проходится по каждому товару из переменной 
    `watch_remnants`, и выбирает только те товары, где артикулы присутствуют в списке `offer_ids`. Вычисляются остатки
    по принципу: Если указано >10, выставляется остаток 100. Если указано как 1, 
    выставляется остаток 0. В остальных случаях берется точное значение. Для оставшихся 
    актикулов, не встречающихся в watch_remnants выставляется остаток 0. 
    
    Agrs: 
    watch_remnants(List[dict]): Список словарей, каждый из которых содержит информацию о конкретном товаре
    offer_ids(List[str]): Список артикулов товаров, зарегистрированных на сайте Yandex.Market .
    warehouse_id(str): Идентификатор склада, к которому привязаны остатки.

    Returns:
    stocks(List[dict]):  Список словарей, каждый из которых содержит информацию о товаре, его остатке и идентификатор склада 
    к которому привязан, дата и время последнего обновления
     
    Exceptions: 
    TypeError: Если `watch_remnants` или `offer_ids` имеют неправильный тип данных.   
    ValueError: Если какая-либо запись в `watch_remnants` содержит недопустимые или неполные данные.
    
    Пример: 
    stocks =  [{'sku': 'Asd', 'warehouseId': 'VB21', items:[{'count': 2, 'type': 'FIT', 'updatedAt': 12.03.2024}]}... """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """ Создает список цен товаров на основе данных из переменной `watch_remnants` и списке артикулов 
    из переменной `offer_ids`. Проходится по списку товаров(`watch_remnants`), фильтруя только те 
    товары, чей артикул указан в переменой (`offer_ids`). Создается переменная (`price`) которая 
    содержит информацию о цене, валюте. Переводит оригинальную цену в нужный формат и округляется до целого числа в функции 
    (`price_conversion`) 
    
    Args: 
    watch_remnants(List[dict]):  Список словарей, каждый из которых содержит информацию о конкретном товаре
    offer_ids(List[str]): Список артикулов товаров, зарегистрированных на Yandex.Market.
    
    Returns:
    prices(List[dict]): Список словарей, каждый из которых содержит информацию о цене товара 

    Exceptions: 
    TypeError: Если `watch_remnants` или `offer_ids` имеют неправильный тип данных.   
    ValueError: Если какая-либо запись в `watch_remnants` содержит недопустимые или неполные данные. 
    KeyError: Появится, если в словаре из `watch_remnants` отсутствуют обязательные поля ('Код' или 'Цена')
    
    Пример: 
    prices =  [ {'id': 'T123', 'price': {'value': 1235, 'currencyId': 'RUR'}}""" 
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token): 
    """ Загружает цены товаров  путем обновления ценовых предложений. 
    Создает цены на основе полученных остатков и обновляет существующие цены по 500 записей.

    Args: 
    watch_remnants(List[dict]): Список словарей, каждый из которых содержит информацию о конкретном товаре 
    campaign_id(str): Идентификатор продавца для получения доступа к своему магазину на сайте Yandex.Market
    market_token(str): Токен аутентификации на Yandex.Market
     
    Returns: 
    list: Список созданных цен 
    
    Exceptions: 
    HTTPError: Возникает, если сервер вернул ошибку (код статуса >= 400) 
    TypeError: Если `watch_remnants` или `offer_ids` имеют неправильный тип данных. """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id): 
    """ Загружает остатки товаров. Формирует остатки на основе текущих запасов товара
    и обновляет эти остатки. Создаются два списка: (`not_empty`) список объектов, где запас 
    не равен нулю. (`stocks`) полный список всех загруженных объектов. Отправляется на Yandex.Market
    пакетами по 2000 элементов. 

    Args: 
    watch_remnants(List[dict]): Список словарей, каждый из которых содержит информацию о конкретном товаре 
    campaign_id(str): Идентификатор продавца для получения доступа к своему магазину 
    market_token(str): Токен аутентификации на Yandex.Market
    warehouse_id(str): Токен аутентифиуации склада где находится товар
    
    Returns: 
    not_empty(list): Отфильтрованный список остатков, где колличество товара не равно нулю 
    stocks(list): Полный список всех загруженных объектов
    
    Exceptions: 
    HTTPError: Возникает, если сервер вернул ошибку (код статуса >= 400) 
    TypeError: Если `watch_remnants` или `offer_ids` имеют неправильный тип данных. """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main(): 
    """ Основная точка входа программы для обновления информации о товарах на Yandex.Market
    Эта функция выполняет полное обновление данных о товарах на сайте Yandex.Market для двух 
    видов ведения продаж на данном сайте('FBS'(Доставка Yandex),'DBS'(Доставка силами продавца)): 
    1.Чтение настроек из переменных окружения. 
    2.Загрузка текущего состояния запасов товаров.
    3.Обновление остатков и цен для обоих видов ведения деятельности.
    4.Отлавливание и обработка возможных ошибок при взаимодействии с API.

    Returns: 
    None: Функция не возвращает никакого значения. 

    Exceptions:
    Все возможные ошибки отображаются в консоли с соответствующим сообщением. 
    
      """
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
