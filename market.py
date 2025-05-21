import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """
    Получает список товаров из кампании на Яндекс Маркете.

    Аргументы:
        page (str): Токен страницы для постраничной загрузки.
        campaign_id (str): Идентификатор кампании.
        access_token (str): OAuth-токен авторизации.

    Возвращает:
        dict: Словарь с результатами запроса (товары, пагинация и т.д.).

    Пример:
        get_product_list("", "123456", "ya_oauth_token")

    Некорректный пример:
        get_product_list(123, None, 456)
    """
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
    """
    Обновляет остатки товаров в кампании Яндекс Маркета.

    Аргументы:
        stocks (list): Список товаров с остатками.
        campaign_id (str): Идентификатор кампании.
        access_token (str): OAuth-токен.

    Возвращает:
        dict: Ответ от API Яндекс Маркета.

    Пример:
        update_stocks([{"sku": "123", "warehouseId": 1, "items": [...] }], "123456", "token")

    Некорректный пример:
        update_stocks("not a list", "123456", None)
    """

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
    """
    Загружает цены на товары в кампанию Яндекс Маркета.

    Аргументы:
        prices (list): Список товаров с ценами.
        campaign_id (str): Идентификатор кампании.
        access_token (str): OAuth-токен.

    Возвращает:
        dict: Ответ от API.

    Пример:
        update_price([{"id": "abc", "price": {"value": 2990, "currencyId": "RUR"}}], "123", "token")

    Некорректный пример:
        update_price("не список", 123, 456)
    """
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
    """
    Получает список артикулов (shopSku) всех товаров в кампании.

    Аргументы:
        campaign_id (str): Идентификатор кампании.
        market_token (str): OAuth-токен.

    Возвращает:
        list: Список артикулов товаров.

    Пример:
        get_offer_ids("123456", "valid_token")

    Некорректный пример:
        get_offer_ids(None, 789)
    """
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
    """
    Формирует структуру остатков товаров для загрузки в Яндекс Маркет.

    Аргументы:
        watch_remnants (list): Локальные данные об остатках.
        offer_ids (list): Список артикулов из Яндекс Маркета.
        warehouse_id (str): Идентификатор склада.

    Возвращает:
        list: Список товаров с остатками в нужном формате.

    Пример:
        create_stocks([{"Код": "123", "Количество": "5"}], ["123"], "456")

    Некорректный пример:
        create_stocks("плохие данные", [], None)
    """
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
    """
    Формирует структуру цен товаров для загрузки в Яндекс Маркет.

    Аргументы:
        watch_remnants (list): Локальные данные о товарах.
        offer_ids (list): Список артикулов из Яндекс Маркета.

    Возвращает:
        list: Список словарей с ценами.

    Пример:
        create_prices([{"Код": "123", "Цена": "1990"}], ["123"])

    Некорректный пример:
        create_prices({}, "не список")
    """
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
    """
    Асинхронно загружает цены товаров в кампанию Яндекс Маркета.

    Аргументы:
        watch_remnants (list): Локальные данные о товарах.
        campaign_id (str): Идентификатор кампании.
        market_token (str): OAuth-токен.

    Возвращает:
        list: Список загруженных цен.

    Пример:
        await upload_prices([{"Код": "111", "Цена": "3000"}], "123", "token")

    Некорректный пример:
        await upload_prices("строка", None, 123)
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """
    Асинхронно загружает остатки товаров в кампанию Яндекс Маркета.

    Аргументы:
        watch_remnants (list): Локальные данные об остатках.
        campaign_id (str): Идентификатор кампании.
        market_token (str): OAuth-токен.
        warehouse_id (str): Идентификатор склада.

    Возвращает:
        tuple: Кортеж из двух списков:
            - Товары с ненулевыми остатками,
            - Все товары с остатками.

    Пример:
        await upload_stocks([{"Код": "111", "Количество": "3"}], "123", "token", "456")

    Некорректный пример:
        await upload_stocks({}, [], None, None)
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    """
    Основная функция: загружает цены и остатки в кампании FBS и DBS на Яндекс Маркете.

    Использует переменные окружения для авторизации и настройки идентификаторов кампаний/складов.

    Пример:
        if __name__ == "__main__":
            main()
    Некорректный пример:
        main(123)  # Функция не принимает аргументы
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