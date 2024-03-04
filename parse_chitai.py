import json
from datetime import datetime
from logging import Logger
import concurrent.futures

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from settings import ASSETS_DIR


class Chitai:

    def __init__(self, logger: Logger):
        self._logger = logger
        self._assets_name = 'chitai'
        self._base_url = 'https://www.chitai-gorod.ru/'
        self._articles_list = None
        self._read_articles()
        self._cookies = {
    '__ddg1_': '2VXLHI0kEVc58Ik8Hj7J',
    'refresh-token': '',
    'tmr_lvid': '0d99a9ecfa17fe8aaa7b44ed0e367163',
    'tmr_lvidTS': '1706361588673',
    '_ga': 'GA1.1.870661923.1709138235',
    '_ym_uid': '1706361589968627328',
    '_ym_d': '1709138235',
    'chg_visitor_id': 'b26157f5-7a3d-47f0-8d6d-49315e04a52b',
    '_gpVisits': '{"isFirstVisitDomain":true,"idContainer":"100025BD"}',
    'popmechanic_sbjs_migrations': 'popmechanic_1418474375998%3D1%7C%7C%7C1471519752600%3D1%7C%7C%7C1471519752605%3D1',
    'access-token': 'Bearer%20eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MDk3MDQxNzcsImlhdCI6MTcwOTUzNjE3NywiaXNzIjoiL2FwaS92MS9hdXRoL2Fub255bW91cyIsInN1YiI6IjU5OTY3NzVlNDE1M2JlMWU5ODk4OWViYjY5MDVjZTU4NGE4N2VhZDZiYjk2YTAyNDkwODVjYTc4MTQyOTJkOTAiLCJ0eXBlIjoxMH0.e8xnl5NKFe9T1v3NHfTa3GHVdy6xbgHxjIGWfoW9RLc',
    '_ym_isad': '1',
    '_ga_6JJPBGS8QY': 'GS1.1.1709536178.2.1.1709536187.0.0.0',
    'digi_uc': 'W1sidiIsIjI5NzI4NDIiLDE3MDk1MzYxNzkwNjNdLFsidiIsIlxuICAgICAgICAyOTcyODQyXG4gICAgICAiLDE3MDk1MzYxODgwMjhdXQ==',
    '_gp100025BD': '{"hits":2,"vc":1,"ac":1,"a6":1}',
    '_ga_LN4Z31QGF4': 'GS1.1.1709536178.3.1.1709536189.49.0.1758121095',
    'mindboxDeviceUUID': 'd83b2a13-9866-425f-a6bc-5236b68f5745',
    'directCrm-session': '%7B%22deviceGuid%22%3A%22d83b2a13-9866-425f-a6bc-5236b68f5745%22%7D',
}
        self._headers = {
    'authority': 'web-gate.chitai-gorod.ru',
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MDk3MDQxNzcsImlhdCI6MTcwOTUzNjE3NywiaXNzIjoiL2FwaS92MS9hdXRoL2Fub255bW91cyIsInN1YiI6IjU5OTY3NzVlNDE1M2JlMWU5ODk4OWViYjY5MDVjZTU4NGE4N2VhZDZiYjk2YTAyNDkwODVjYTc4MTQyOTJkOTAiLCJ0eXBlIjoxMH0.e8xnl5NKFe9T1v3NHfTa3GHVdy6xbgHxjIGWfoW9RLc',
    'cache-control': 'no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0',
    # 'cookie': '__ddg1_=2VXLHI0kEVc58Ik8Hj7J; refresh-token=; tmr_lvid=0d99a9ecfa17fe8aaa7b44ed0e367163; tmr_lvidTS=1706361588673; _ga=GA1.1.870661923.1709138235; _ym_uid=1706361589968627328; _ym_d=1709138235; chg_visitor_id=b26157f5-7a3d-47f0-8d6d-49315e04a52b; _gpVisits={"isFirstVisitDomain":true,"idContainer":"100025BD"}; popmechanic_sbjs_migrations=popmechanic_1418474375998%3D1%7C%7C%7C1471519752600%3D1%7C%7C%7C1471519752605%3D1; access-token=Bearer%20eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MDk3MDQxNzcsImlhdCI6MTcwOTUzNjE3NywiaXNzIjoiL2FwaS92MS9hdXRoL2Fub255bW91cyIsInN1YiI6IjU5OTY3NzVlNDE1M2JlMWU5ODk4OWViYjY5MDVjZTU4NGE4N2VhZDZiYjk2YTAyNDkwODVjYTc4MTQyOTJkOTAiLCJ0eXBlIjoxMH0.e8xnl5NKFe9T1v3NHfTa3GHVdy6xbgHxjIGWfoW9RLc; _ym_isad=1; _ga_6JJPBGS8QY=GS1.1.1709536178.2.1.1709536187.0.0.0; digi_uc=W1sidiIsIjI5NzI4NDIiLDE3MDk1MzYxNzkwNjNdLFsidiIsIlxuICAgICAgICAyOTcyODQyXG4gICAgICAiLDE3MDk1MzYxODgwMjhdXQ==; _gp100025BD={"hits":2,"vc":1,"ac":1,"a6":1}; _ga_LN4Z31QGF4=GS1.1.1709536178.3.1.1709536189.49.0.1758121095; mindboxDeviceUUID=d83b2a13-9866-425f-a6bc-5236b68f5745; directCrm-session=%7B%22deviceGuid%22%3A%22d83b2a13-9866-425f-a6bc-5236b68f5745%22%7D',
    'origin': 'https://www.chitai-gorod.ru',
    'referer': 'https://www.chitai-gorod.ru/',
    'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
}
        self._chunk_size = 10000
        self._books_info = []
        self._error_books = []

    def _read_articles(self):
        df = pd.read_excel(ASSETS_DIR / 'input' / f'{self._assets_name}.xlsx')
        self._articles_list = df['Артикул'].to_list()
        self._logger.info(f'[Labirint:_read_articles] - Got {len(self._articles_list)} articles')

    def _fetch_book_info(self, book_id: str):
        try:
            params = {
                'filters[ids]': [book_id],
                'products[page]': '1',
                'products[per-page]': '30',
                'sortPreset': 'relevance',
                'include': 'productTexts, publisher, publisherBrand, publisherSeries, dates, literatureWorkCycles, rating',
            }
            response = requests.get('https://web-gate.chitai-gorod.ru/api/v2/products', params=params,
                                    cookies=self._cookies, headers=self._headers)
            data = response.json()
            url = self._base_url + data.get('data')[0].get('attributes').get('url')
            name = data.get('data')[0].get('attributes').get('title')
            availability = data.get('data')[0].get('attributes').get('status')

            if availability == 'canBuy':
                response = requests.get(url, cookies=self._cookies, headers=self._headers)
                soup = BeautifulSoup(response.text, 'lxml')
                availability = soup.find('div', class_='product-detail-offer__availability').text.strip()
            elif availability == 'offline':
                availability = 'Нет в наличии'
            elif availability == 'canSubscribe':
                availability = 'Буду ждать'

            headers = {
    'authority': 'web-gate.chitai-gorod.ru',
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MDk3MDQxNzcsImlhdCI6MTcwOTUzNjE3NywiaXNzIjoiL2FwaS92MS9hdXRoL2Fub255bW91cyIsInN1YiI6IjU5OTY3NzVlNDE1M2JlMWU5ODk4OWViYjY5MDVjZTU4NGE4N2VhZDZiYjk2YTAyNDkwODVjYTc4MTQyOTJkOTAiLCJ0eXBlIjoxMH0.e8xnl5NKFe9T1v3NHfTa3GHVdy6xbgHxjIGWfoW9RLc',
    'cache-control': 'no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0',
    # 'cookie': '__ddg1_=2VXLHI0kEVc58Ik8Hj7J; refresh-token=; tmr_lvid=0d99a9ecfa17fe8aaa7b44ed0e367163; tmr_lvidTS=1706361588673; _ga=GA1.1.870661923.1709138235; _ym_uid=1706361589968627328; _ym_d=1709138235; chg_visitor_id=b26157f5-7a3d-47f0-8d6d-49315e04a52b; _gpVisits={"isFirstVisitDomain":true,"idContainer":"100025BD"}; popmechanic_sbjs_migrations=popmechanic_1418474375998%3D1%7C%7C%7C1471519752600%3D1%7C%7C%7C1471519752605%3D1; access-token=Bearer%20eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MDk3MDQxNzcsImlhdCI6MTcwOTUzNjE3NywiaXNzIjoiL2FwaS92MS9hdXRoL2Fub255bW91cyIsInN1YiI6IjU5OTY3NzVlNDE1M2JlMWU5ODk4OWViYjY5MDVjZTU4NGE4N2VhZDZiYjk2YTAyNDkwODVjYTc4MTQyOTJkOTAiLCJ0eXBlIjoxMH0.e8xnl5NKFe9T1v3NHfTa3GHVdy6xbgHxjIGWfoW9RLc; _ym_isad=1; mindboxDeviceUUID=d83b2a13-9866-425f-a6bc-5236b68f5745; directCrm-session=%7B%22deviceGuid%22%3A%22d83b2a13-9866-425f-a6bc-5236b68f5745%22%7D; _ga_6JJPBGS8QY=GS1.1.1709536178.2.1.1709536187.0.0.0; digi_uc=W1sidiIsIjI5NzI4NDIiLDE3MDk1MzYxNzkwNjNdLFsidiIsIlxuICAgICAgICAyOTcyODQyXG4gICAgICAiLDE3MDk1MzYxODgwMjhdXQ==; _ga_LN4Z31QGF4=GS1.1.1709536178.3.1.1709536188.50.0.1758121095; _gp100025BD={"hits":2,"vc":1,"ac":1,"a6":1}',
    'origin': 'https://www.chitai-gorod.ru',
    'referer': 'https://www.chitai-gorod.ru/',
    'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
}

            params = {
                'city': '213',
            }
            response = requests.get(
                f'https://web-gate.chitai-gorod.ru/api/v1/availability/{book_id}',
                params=params,
                cookies=self._cookies,
                headers=headers,
            )
            shop_ex = response.json().get('data').get('points')
            if shop_ex:
                availability += f'. В наличии в {len(shop_ex)} магазинах'

            self._books_info.append({
                'Ссылка': url,
                'Название': name,
                'ID': book_id,
                'Наличие': availability,
            })
        except Exception as e:
            self._logger.error(f'[Chitai:_fetch_book_info] - No book with id: {book_id}. Error: {e}')
            self._error_books.append(book_id)
            return None

    def _save_to_json(self, chunk_number: int):
        filename = ASSETS_DIR / 'pre_final' / self._assets_name / f'{self._assets_name}_chunk_{chunk_number}.json'
        with open(filename, 'w', encoding='utf-8') as file:
            json.dump(self._books_info, file, ensure_ascii=False, indent=4)

    def _parse_chunk(self, current_chunk: list, chunk_number: int):
        self._books_info = []

        def fetch_book_info_parallel(article):
            self._fetch_book_info(book_id=article)

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            list(tqdm(executor.map(fetch_book_info_parallel, current_chunk), total=len(current_chunk)))

        self._save_to_json(chunk_number=chunk_number)

    def _save_to_excel(self):
        import os

        folder_path = ASSETS_DIR / 'pre_final' / 'chitai'
        json_files = [f for f in os.listdir(folder_path) if f.endswith('.json')]

        combined_df = pd.DataFrame()

        for json_file in tqdm(json_files):
            file_path = os.path.join(folder_path, json_file)

            df = pd.read_json(file_path, orient='records')

            combined_df = pd.concat([combined_df, df], ignore_index=True)

        formatted_date = datetime.now().strftime("%Y-%m-%d")
        combined_df.to_excel(ASSETS_DIR / 'results' / f'chitai_result_{formatted_date}.xlsx', index=False)

    def parse(self):
        total_articles = len(self._articles_list)
        folder_path = ASSETS_DIR / 'pre_final' / self._assets_name
        files = list(folder_path.glob('*'))
        file_count = sum(1 for f in files if f.is_file())
        if file_count > 0:
            self._logger.info(f'[Chitai:parse] - Already parsed {file_count} steps')
        for chunk_number, start in enumerate(range(file_count * self._chunk_size, total_articles, self._chunk_size),
                                             start=file_count + 1):
            self._logger.info(
                f'[Labirint:parse] - Step {chunk_number}: {start}-{start + self._chunk_size}/{total_articles}')
            end = min(start + self._chunk_size, total_articles)
            current_chunk = self._articles_list[start:end]
            self._parse_chunk(current_chunk, chunk_number)

        with open('errors.json', 'w', encoding='utf-8') as file:
            json.dump(self._error_books, file, ensure_ascii=False, indent=4)

        self._save_to_excel()


if __name__ == '__main__':
    import logging


    def configure_logger(logger: logging.Logger):
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)


    logger = logging.getLogger('Books Parser')
    configure_logger(logger)

    chitai = Chitai(logger)
    chitai.parse()
    # chitai._fetch_book_info('2972842')
