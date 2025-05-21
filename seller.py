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
    """
    Получает список товаров магазина Ozon.

    Аргументы:
        last_id (str): Последний ID, с которого продолжить загрузку товаров.
        client_id (str): Идентификатор клиента Ozon.
        seller_token (str): Токен доступа продавца.

    Возвращает:
        dict: Результат API-запроса со списком товаров.

    Пример:
        >>> get_product_list("", "12345", "abcde")  # Вернет словарь с товарами

    Некорректный пример:
        >>> get_product_list("", None, "abcde")
        Traceback (most recent call last):
            ...
        requests.exceptions.HTTPError
    """
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
    """
    Получает список артикулов (offer_id) всех товаров продавца.

    Аргументы:
        client_id (str): Идентификатор клиента Ozon.
        seller_token (str): Токен доступа продавца.

    Возвращает:
        list: Список артикулов товаров.

    Пример:
        >>> get_offer_ids("12345", "abcde")
        ['001', '002', '003']

    Некорректный пример:
        >>> get_offer_ids("", "")
        Traceback (most recent call last):
            ...
        requests.exceptions.HTTPError
    """
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
    """
        Обновляет цены на товары в Ozon.

        Аргументы:
            prices (list): Список словарей с ценами.
            client_id (str): Идентификатор клиента Ozon.
            seller_token (str): Токен доступа продавца.

        Возвращает:
            dict: Ответ от API Ozon.

        Пример:
            >>> update_price([{'offer_id': '123', 'price': '999'}], "id", "token")
            {'result': 'success'}

        Некорректный пример:
            >>> update_price("invalid", "id", "token")
            Traceback (most recent call last):
                ...
            TypeError
        """
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
    """
        Обновляет остатки товаров на складе в Ozon.

        Аргументы:
            stocks (list): Список словарей с остатками.
            client_id (str): Идентификатор клиента Ozon.
            seller_token (str): Токен доступа продавца.

        Возвращает:
            dict: Ответ от API Ozon.

        Пример:
            >>> update_stocks([{'offer_id': '123', 'stock': 5}], "id", "token")
            {'result': 'success'}

        Некорректный пример:
            >>> update_stocks("invalid", "id", "token")
            Traceback (most recent call last):
                ...
            TypeError
        """
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
    """
        Загружает и извлекает файл с остатками часов с сайта Casio.

        Возвращает:
            list: Список остатков товаров в формате словарей.

        Пример:
            >>> remnants = download_stock()
            >>> remnants[0]['Код']
            '12345'

        Некорректный пример:
            >>> download_stock("wrong_arg")
            Traceback (most recent call last):
                ...
            TypeError
        """
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
    """
        Создает список остатков по артикулу на основе загруженных данных.

        Аргументы:
            watch_remnants (list): Список остатков из Excel-файла.
            offer_ids (list): Список артикулов, загруженных в Ozon.

        Возвращает:
            list: Список остатков для загрузки в Ozon.

        Пример:
            >>> create_stocks([{'Код': '123', 'Количество': '>10'}], ['123'])
            [{'offer_id': '123', 'stock': 100}]

        Некорректный пример:
            >>> create_stocks(None, None)
            Traceback (most recent call last):
                ...
            TypeError
        """
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
    """
    Создает список цен на товары по артикулу.

    Аргументы:
        watch_remnants (list): Остатки и цены из Excel-файла.
        offer_ids (list): Список артикулов для обновления.

    Возвращает:
        list: Список словарей с ценами для API Ozon.

    Пример:
        >>> create_prices([{'Код': '123', 'Цена': "5'990.00 руб."}], ['123'])
        [{'offer_id': '123', 'price': '5990', ...}]

    Некорректный пример:
        >>> create_prices(None, None)
        Traceback (most recent call last):
            ...
        TypeError
    """
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
    """
        Преобразует строку с ценой, содержащую форматирование и символы валюты, в числовую строку без лишних символов.

        Аргументы:
            price (str): Строка с ценой для преобразования.
                Пример допустимого ввода: "5'990.00 руб."

        Возвращает:
            str: Строка, содержащая только числовую часть цены без форматирования.
                Пример: "5990"

        Пример:
            >>> price_conversion("5'990.00 руб.")
            '5990'

        Некорректный пример:
            >>> price_conversion(None)
            Traceback (most recent call last):
                ...
            AttributeError: 'NoneType' object has no attribute 'split'
        """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """
    Разбивает список на подсписки длиной не более n элементов.

    Аргументы:
        lst (list): Исходный список.
        n (int): Размер каждого блока.

    Возвращает:
        generator: Генератор подсписков.

    Пример:
        >>> list(divide([1, 2, 3, 4], 2))
        [[1, 2], [3, 4]]

    Некорректный пример:
        >>> list(divide("not_a_list", 2))
        Traceback (most recent call last):
            ...
        TypeError
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """
    Загружает цены товаров в Ozon.

    Аргументы:
        watch_remnants (list): Список данных по товарам.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца.

    Возвращает:
        list: Список загруженных цен.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """
    Загружает остатки товаров в Ozon.

    Аргументы:
        watch_remnants (list): Список остатков товаров.
        client_id (str): Идентификатор клиента.
        seller_token (str): Токен продавца.

    Возвращает:
        tuple: Кортеж из списков (ненулевые остатки, все остатки).
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """
    Основной скрипт: загружает остатки с сайта Casio и обновляет данные в магазине Ozon.
    """
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
