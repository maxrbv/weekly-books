from logging import Logger
import json
import concurrent.futures
from datetime import datetime
import os

import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm

from utils.target_logger import get_logger
from utils.utils import get_session
from settings import ASSETS_DIR


logger = get_logger(name='Labirint', session_id='Labirint')


class Labirint:

    def __init__(self, logger: Logger):
        self._logger = logger

        self._assets_name = 'labirint'
        self._pre_final_dir = ASSETS_DIR / 'pre_final' / self._assets_name
        self._base_url = 'https://www.labirint.ru/'
        self._parse_url = 'https://www.labirint.ru/books/'
        self._read_articles()

        self._session = get_session()
        self._cookies = {
            'visit_count': '1',
            'PHPSESSID': '9o4pde5qe1s8n69loi6t0k1dgn',
            'id_post': '2451',
            'UserSes': 'lab9o4pde5qe1s8n69',
            'br_webp': '8',
            'begintimed': 'MTcwMjc2OTgwNg%3D%3D',
            'fotorama-item-790566': '0',
            '_ga_21PJ900698': 'GS1.1.1702769807.1.0.1702769807.0.0.0',
            '_ga': 'GA1.2.1100216345.1702769808',
            '_gid': 'GA1.2.164823631.1702769808',
            '_dc_gtm_UA-3229737-1': '1',
            '_ym_uid': '1702769808552808847',
            '_ym_d': '1702769808',
            'st_uid': '82076b79dbcf5f60737026f3bd4f7c24',
            'tmr_lvid': 'e9a77b749b213444810e4770875ef198',
            'tmr_lvidTS': '1702769808113',
            '_ym_visorc': 'b',
            '_ym_isad': '2',
            'adrdel': '1',
            'adrcid': 'Amfk8E_0AIDTIhsUusCscBg',
            'tmr_detect': '0%7C1702769810534',
            'cookie_policy': '1',
        }
        self._headers = {
            'authority': 'www.labirint.ru',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'cache-control': 'max-age=0',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        }

        self._chunk_size = 100000000
        self._get_chunks_parsed()

        self._workers = 10

    def _read_articles(self):
        df = pd.read_excel(ASSETS_DIR / 'input' / f'{self._assets_name}.xlsx')
        df['Артикул'] = df['Артикул'].str.replace("'", "")
        self._articles_list = df['Артикул'].to_list()
        self._logger.info(f'[Labirint:_read_articles] - Got {len(self._articles_list)} articles')

    def _get_chunks_parsed(self):
        files = list(self._pre_final_dir.glob('*'))
        self._file_count = sum(1 for f in files if f.is_file())
        if self._file_count > 0:
            self._logger.info(f'[Labirint:parse] - Already parsed {self._file_count} steps')

    def _fetch_book_info(self, article: str):
        url = f'https://www.labirint.ru/books/{article}/'
        try:
            response = self._session.get(url, cookies=self._cookies, headers=self._headers)
            soup = BeautifulSoup(response.text, 'lxml')

            try:
                name = soup.find('div', {'id': 'product-title'}).find('h1').text.strip()
            except:
                name = None

            try:
                availability = soup.find('div', class_='prodtitle-availibility').find('span').text.strip()
            except:
                availability = 'Нет в продаже'

            self._books_info.append(
                {
                    'Ссылка': url,
                    'Название': name,
                    'ID': article,
                    'Наличие': availability,
                }
            )
        except Exception as e:
            self._logger.error(f'[Labirint:_fetch_book_info] - No book with id: {article}. Error: {e}')

    def _save_to_json(self, chunk_number: int):
        filename = ASSETS_DIR / 'pre_final' / self._assets_name / f'{self._assets_name}_chunk_{chunk_number}.json'
        with open(filename, 'w', encoding='utf-8') as file:
            json.dump(self._books_info, file, ensure_ascii=False, indent=4)

    def _save_to_excel(self):
        folder_path = ASSETS_DIR / 'pre_final' / 'labirint'
        json_files = [f for f in os.listdir(folder_path) if f.endswith('.json')]

        combined_df = pd.DataFrame()

        for json_file in tqdm(json_files):
            file_path = os.path.join(folder_path, json_file)

            df = pd.read_json(file_path, orient='records')

            combined_df = pd.concat([combined_df, df], ignore_index=True)

        formatted_date = datetime.now().strftime("%Y-%m-%d")
        combined_df.to_excel(ASSETS_DIR / 'results' / f'labirint_result_{formatted_date}.xlsx', index=False)

    def _parse_chunk(self, current_chunk: list, chunk_number: int):
        self._books_info = []

        def fetch_book_info_parallel(article):
            self._fetch_book_info(article=article)

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            list(tqdm(executor.map(fetch_book_info_parallel, current_chunk), total=len(current_chunk)))

        self._save_to_json(chunk_number=chunk_number)

    def parse(self):
        total_articles = len(self._articles_list)
        for chunk_number, start in enumerate(range(self._file_count * self._chunk_size, total_articles, self._chunk_size), start=self._file_count+1):
            self._logger.info(f'[Labirint:parse] - Step {chunk_number}: {start}-{start + self._chunk_size}/{total_articles}')
            end = min(start + self._chunk_size, total_articles)
            current_chunk = self._articles_list[start:end]
            self._parse_chunk(current_chunk, chunk_number)

        self._save_to_excel()


if __name__ == '__main__':
    labirint = Labirint(logger=logger)
    labirint.parse()
