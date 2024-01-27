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
            '__ddg1_': 'r6HypZ7lTQmcZWfDVdME',
            'refresh-token': '',
            'access-token': 'Bearer%20eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MDY0NjcwNjQsImlhdCI6MTcwNjI5OTA2NCwiaXNzIjoiL2FwaS92MS9hdXRoL2Fub255bW91cyIsInN1YiI6IjRjNjJlNTgyMGYzNzI3N2ZkNDQ1MTZiYTIxNzMwODNlZWU5MTA0ZjlmNmRhYTA4ZWNmYjk4MGZhMzcwZmE1NmYiLCJ0eXBlIjoxMH0.GlDDl6ddPkPRDnefmSWenmYiIj486QZG8hQ33v_IgWk',
            '_ga': 'GA1.1.2095788095.1706299066',
            'tmr_lvid': '095bdb4e343f2b80995fb44fa12d7e24',
            'tmr_lvidTS': '1702778231858',
            '_ym_uid': '1702778232819912034',
            '_ym_d': '1706299066',
            'chg_visitor_id': '3ec5d99d-62e1-4921-b7d3-d43a5fa5cf41',
            '_gpVisits': '{"isFirstVisitDomain":true,"idContainer":"100025BD"}',
            '_ym_isad': '1',
            'popmechanic_sbjs_migrations': 'popmechanic_1418474375998%3D1%7C%7C%7C1471519752600%3D1%7C%7C%7C1471519752605%3D1',
            'digi_uc': 'W1sidiIsIjI5NzI4NDIiLDE3MDYzMzc1OTQwOTddLFsidiIsIlxuICAgICAgICAzMDIyNzc2XG4gICAgICAiLDE3MDYyOTkwNjU5NjddLFsidiIsIlxuICAgICAgICAyOTcyODQyXG4gICAgICAiLDE3MDYzMzc2MTcyNDhdXQ==',
            '_gp100025BD': '{"hits":4,"vc":1,"ac":1,"a6":1}',
            '_ga_LN4Z31QGF4': 'GS1.1.1706337593.3.1.1706337618.35.0.0',
            'mindboxDeviceUUID': 'e1a4aabd-63c9-473a-a727-4ae5b50ace72',
            'directCrm-session': '%7B%22deviceGuid%22%3A%22e1a4aabd-63c9-473a-a727-4ae5b50ace72%22%7D',
        }
        self._headers = {
            'authority': 'web-gate.chitai-gorod.ru',
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MDYyMzAxNTUsImlhdCI6MTcwNjA2MjE1NSwiaXNzIjoiL2FwaS92MS9hdXRoL2Fub255bW91cyIsInN1YiI6IjMwZGRkYzFmMDJkODFlMDI4NjY1YmRjMTIzN2Y0MTU1MGQ4MzA5Y2U1NGI0ZDYxM2JlYzdhYjdhMGIzY2ExMGMiLCJ0eXBlIjoxMH0.YdM13D0HR_X9e7hHH5OSQQq11KyluaTzN503JPZLQxQ',
            'cache-control': 'no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0',
            # 'cookie': '__ddg1_=wU2qwWkFRKhKuW3J9sIt; refresh-token=; access-token=Bearer%20eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MDYyMzAxNTUsImlhdCI6MTcwNjA2MjE1NSwiaXNzIjoiL2FwaS92MS9hdXRoL2Fub255bW91cyIsInN1YiI6IjMwZGRkYzFmMDJkODFlMDI4NjY1YmRjMTIzN2Y0MTU1MGQ4MzA5Y2U1NGI0ZDYxM2JlYzdhYjdhMGIzY2ExMGMiLCJ0eXBlIjoxMH0.YdM13D0HR_X9e7hHH5OSQQq11KyluaTzN503JPZLQxQ; _ga=GA1.1.430841412.1706062157; tmr_lvid=095bdb4e343f2b80995fb44fa12d7e24; tmr_lvidTS=1702778231858; digi_uc=W1sidiIsIlxuICAgICAgICAyOTcyODQyXG4gICAgICAiLDE3MDYwNjIxNTY4OThdXQ==; _ym_uid=1702778232819912034; _ym_d=1706062157; chg_visitor_id=5edc33a2-5f0e-43c3-8d02-174871342241; _ym_isad=1; _gpVisits={"isFirstVisitDomain":true,"idContainer":"100025BD"}; popmechanic_sbjs_migrations=popmechanic_1418474375998%3D1%7C%7C%7C1471519752600%3D1%7C%7C%7C1471519752605%3D1; _ga_LN4Z31QGF4=GS1.1.1706062156.1.0.1706062158.58.0.0; mindboxDeviceUUID=e1a4aabd-63c9-473a-a727-4ae5b50ace72; directCrm-session=%7B%22deviceGuid%22%3A%22e1a4aabd-63c9-473a-a727-4ae5b50ace72%22%7D; _gp100025BD={"hits":1,"vc":1,"ac":1}',
            'origin': 'https://www.chitai-gorod.ru',
            'referer': 'https://www.chitai-gorod.ru/',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
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
                'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MDY0NjcwNjQsImlhdCI6MTcwNjI5OTA2NCwiaXNzIjoiL2FwaS92MS9hdXRoL2Fub255bW91cyIsInN1YiI6IjRjNjJlNTgyMGYzNzI3N2ZkNDQ1MTZiYTIxNzMwODNlZWU5MTA0ZjlmNmRhYTA4ZWNmYjk4MGZhMzcwZmE1NmYiLCJ0eXBlIjoxMH0.GlDDl6ddPkPRDnefmSWenmYiIj486QZG8hQ33v_IgWk',
                'cache-control': 'no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0',
                # 'cookie': '__ddg1_=r6HypZ7lTQmcZWfDVdME; refresh-token=; access-token=Bearer%20eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MDY0NjcwNjQsImlhdCI6MTcwNjI5OTA2NCwiaXNzIjoiL2FwaS92MS9hdXRoL2Fub255bW91cyIsInN1YiI6IjRjNjJlNTgyMGYzNzI3N2ZkNDQ1MTZiYTIxNzMwODNlZWU5MTA0ZjlmNmRhYTA4ZWNmYjk4MGZhMzcwZmE1NmYiLCJ0eXBlIjoxMH0.GlDDl6ddPkPRDnefmSWenmYiIj486QZG8hQ33v_IgWk; _ga=GA1.1.2095788095.1706299066; tmr_lvid=095bdb4e343f2b80995fb44fa12d7e24; tmr_lvidTS=1702778231858; _ym_uid=1702778232819912034; _ym_d=1706299066; chg_visitor_id=3ec5d99d-62e1-4921-b7d3-d43a5fa5cf41; _gpVisits={"isFirstVisitDomain":true,"idContainer":"100025BD"}; _ym_isad=1; popmechanic_sbjs_migrations=popmechanic_1418474375998%3D1%7C%7C%7C1471519752600%3D1%7C%7C%7C1471519752605%3D1; mindboxDeviceUUID=e1a4aabd-63c9-473a-a727-4ae5b50ace72; directCrm-session=%7B%22deviceGuid%22%3A%22e1a4aabd-63c9-473a-a727-4ae5b50ace72%22%7D; _ga_LN4Z31QGF4=GS1.1.1706361034.4.1.1706361051.43.0.0; digi_uc=W1sidiIsIjI5NzI4NDIiLDE3MDYzNjEwMzUyNjVdLFsidiIsIlxuICAgICAgICAyOTcyODQyXG4gICAgICAiLDE3MDYzNjEwNTEzNTVdLFsidiIsIlxuICAgICAgICAzMDIyNzc2XG4gICAgICAiLDE3MDYyOTkwNjU5NjddXQ==; _gp100025BD={"hits":6,"vc":1,"ac":1,"a6":1}',
                'origin': 'https://www.chitai-gorod.ru',
                'referer': 'https://www.chitai-gorod.ru/',
                'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-site',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
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
