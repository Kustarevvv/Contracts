import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
from typing import Optional, List, Dict
import logging
import random
import math

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ContParser:
    def __init__(self):
        self.domain = "https://zakupki.gov.ru"
        self.base_url = "https://zakupki.gov.ru/epz/contract/search/results.html"

        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        ]

        self.session = requests.Session()
        self.update_user_agent()

        self.params = {
            'morphology': 'on',
            'search-filter': 'Дате размещения',
            'fz44': 'on',
            'contractStageList_1': 'on',
            'contractStageList_2': 'on',
            'contractStageList': '1,2',
            'budgetLevelsIdNameHidden': '{}',
            'customerPlace': '78000000000',
            'customerPlaceCodes': '78000000000',
            'contractDateFrom': '26.08.2020',
            'okpd2Ids': '8891047',
            'okpd2IdsCodes': '81.10.10.000',
            'unitPriceEnd': '250000000',
            'sortBy': 'UPDATE_DATE',
            'pageNumber': '1',
            'sortDirection': 'false',
            'recordsPerPage': '_10',
            'showLotsInfoHidden': 'false'
        }

        self.request_count = 0
        self.failed_attempts = 0  
        self.base_delay = 1.0  
        self.max_delay = 60.0  

    def exponential_backoff(self):
        if self.failed_attempts == 0:
            return 0

        delay = min(self.base_delay * (2 ** (self.failed_attempts - 1)), self.max_delay)

        jitter = random.uniform(0, delay * 0.3)
        total_delay = delay + jitter

        logger.info(f"Экспоненциальная задержка: {total_delay:.2f} сек (попытка {self.failed_attempts})")
        return total_delay

    def safe_sleep(self, delay: float):

        try:
            time.sleep(delay)
        except KeyboardInterrupt:
            logger.info("Задержка прервана пользователем")
            raise

    def update_user_agent(self):

        new_user_agent = random.choice(self.user_agents)
        self.session.headers.update({
            'User-Agent': new_user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        logger.debug("User-Agent изменен")

    def make_absolute_url(self, relative_url: str) -> str:

        if relative_url.startswith('http'):
            return relative_url
        elif relative_url.startswith('/'):
            return self.domain + relative_url
        else:
            return self.domain + '/' + relative_url

    def get_soup(self, url: str, params: Optional[dict] = None, max_retries: int = 5) -> Optional[BeautifulSoup]:

        for attempt in range(max_retries):
            try:

                self.request_count += 1
                if self.request_count % 5 == 0:
                    self.update_user_agent()

                if self.failed_attempts > 0:
                    delay = self.exponential_backoff()
                    self.safe_sleep(delay)

                response = self.session.get(url, params=params, timeout=30)
                response.encoding = 'utf-8'

                if response.status_code == 429:
                    logger.warning(f"429 Too Many Requests. Попытка {attempt + 1}/{max_retries}")
                    self.failed_attempts += 1
                    continue

                response.raise_for_status()

                self.failed_attempts = 0
                return BeautifulSoup(response.text, 'html.parser')

            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:
                    logger.warning(f"HTTP 429. Попытка {attempt + 1}/{max_retries}")
                    self.failed_attempts += 1
                else:
                    logger.error(f"HTTP ошибка {response.status_code if 'response' in locals() else 'N/A'}: {e}")
                    self.failed_attempts += 1

            except requests.RequestException as e:
                logger.error(f"Ошибка сети: {e}")
                self.failed_attempts += 1

            except Exception as e:
                logger.error(f"Неожиданная ошибка: {e}")
                self.failed_attempts += 1

            if attempt < max_retries - 1:
                delay = self.exponential_backoff()
                self.safe_sleep(delay)

        logger.error(f"Не удалось получить данные после {max_retries} попыток")
        return None

    def get_total_pages(self, soup: BeautifulSoup) -> int:

        pagination = soup.find('div', class_='paginator-block')
        if pagination:
            page_links = pagination.find_all('a')
            if page_links and len(page_links) >= 2:
                try:
                    return int(page_links[-2].text.strip())
                except ValueError:
                    logger.warning("Не удалось определить количество страниц")
        return 1

    def extract_contract_data(self, soup: BeautifulSoup, contract_url: str) -> Dict[str, Optional[str]]:

        labels = soup.find_all('span', class_='section__title')

        def find_label_value(label_text: str) -> Optional[str]:

            for label in labels:
                if label.text and label_text in label.text:
                    value = label.find_next_sibling('span')
                    return value.text.strip() if value else None
            return None

        contract_number = find_label_value('Реестровый номер контракта')
        contract_status = find_label_value('Статус контракта')
        notice_number = find_label_value('Номер извещения об осуществлении закупки')
        start_date = find_label_value('Дата заключения контракта')
        end_date = find_label_value('Дата окончания исполнения контракта')
        price = find_label_value('Цена контракта')

        inn_labels = soup.find_all('span', class_='grey-main-light')
        inn_number = None
        for label in inn_labels:
            if label.text and 'ИНН' in label.text:
                inn_value = label.find_next_sibling('span')
                inn_number = inn_value.text.strip() if inn_value else None
                break

        name_element = soup.find('td', class_='tableBlock__col tableBlock__col_first text-break')
        name = None
        if name_element:
            name = name_element.get_text(' ', strip=True)
            for separator in ['ИНН:', 'КПП:']:
                if separator in name:
                    name = name.split(separator)[0].strip()

        return {
            'Реестровый номер контракта': contract_number,
            'ИНН исполнителя': inn_number,
            'Наименование исполнителя': name,
            'Статус контракта': contract_status,
            'Номер извещения': notice_number,
            'Дата заключения контракта': start_date,
            'Дата окончания исполнения контракта': end_date,
            'Цена контракта': price,
            'Ссылка на контракт': contract_url
        }

    def get_execution_data(self, soup: BeautifulSoup) -> Dict[str, Optional[str]]:

        execution_data = {
            'Фактически оплачено': None,
            'Основание расторжения контракта': None,
            'Причина расторжения контракта': None
        }

        execution_link = None
        links = soup.find_all('a', class_='tabsNav__item')
        for link in links:
            if link.text and 'Исполнение (расторжение) контракта' in link.text:
                execution_link = link.get('href')
                break

        if not execution_link:
            return execution_data

        try:
            execution_url = self.make_absolute_url(execution_link)
            soup_execution = self.get_soup(execution_url)

            if not soup_execution:
                return execution_data

            labels = soup_execution.find_all('span', class_='section__title')

            for label in labels:
                if label.text:
                    if 'Фактически оплачено, ₽' in label.text:
                        value = label.find_next_sibling('span')
                        execution_data['Фактически оплачено'] = value.text.strip() if value else None
                    elif 'Основание расторжения контракта' in label.text:
                        value = label.find_next_sibling('span')
                        execution_data['Основание расторжения контракта'] = value.text.strip() if value else None

        except Exception as e:
            logger.warning(f"Ошибка при получении данных исполнения: {e}")

        return execution_data

    def check_unreliable_supplier(self, soup: BeautifulSoup) -> str:
  
        unreliable_link = None
        links = soup.find_all('a')
        for link in links:
            if link.text and 'Недобросовестный поставщик' in link.text:
                unreliable_link = link.get('href')
                break

        if not unreliable_link:
            return 'нет'

        try:
            unreliable_url = self.make_absolute_url(unreliable_link)
            soup_unreliable = self.get_soup(unreliable_url)

            if not soup_unreliable:
                return 'ошибка'

            no_records = soup_unreliable.find('p', class_='noRecords')
            return 'нет' if no_records else 'да'

        except Exception as e:
            logger.warning(f"Ошибка при проверке недобросовестного поставщика: {e}")
            return 'ошибка'

    def parse_contracts(self) -> List[Dict]:

        contracts_data = []

        try:

            soup = self.get_soup(self.base_url, params=self.params)
            if not soup:
                logger.error("Не удалось получить первую страницу")
                return contracts_data

            total_pages = self.get_total_pages(soup)

            logger.info(f"Всего страниц для парсинга: {total_pages}")

            for page in range(1, total_pages + 1):
                logger.info(f"Обрабатывается страница {page} из {total_pages}")

                self.params['pageNumber'] = str(page)
                soup = self.get_soup(self.base_url, params=self.params)

                if not soup:
                    logger.warning(f"Не удалось получить страницу {page}, пропускаем")
                    continue

                contract_elements = soup.find_all('div', class_='col-8 pr-0 mr-21px')

                for contract_element in contract_elements:
                    try:
                        link_element = contract_element.find('a')
                        if not link_element:
                            continue

                        relative_url = link_element.get('href')
                        contract_url = self.make_absolute_url(relative_url)

                        # Получаем основную страницу контракта
                        soup_contract = self.get_soup(contract_url)

                        if not soup_contract:
                            logger.warning(f"Не удалось получить контракт {contract_url}, пропускаем")
                            continue

                        # Извлекаем основные данные
                        contract_data = self.extract_contract_data(soup_contract, contract_url)

                        # Получаем данные об исполнении
                        execution_data = self.get_execution_data(soup_contract)

                        # Проверяем недобросовестного поставщика
                        is_unreliable = self.check_unreliable_supplier(soup_contract)

                        # Объединяем все данные
                        full_data = {**contract_data, **execution_data,
                                     'Включение в реестр недобросовестных поставщиков': is_unreliable}

                        contracts_data.append(full_data)

                        logger.info(f"Обработан контракт: {contract_data.get('Реестровый номер контракта', 'N/A')}")

                        self.safe_sleep(0.5 + random.uniform(0, 0.3))

                    except Exception as e:
                        logger.error(f"Ошибка при обработке контракта: {e}")
                        continue

                self.safe_sleep(1.0 + random.uniform(0, 0.5))

        except Exception as e:
            logger.error(f"Критическая ошибка при парсинге: {e}")

        return contracts_data

    def save_to_excel(self, data: List[Dict], filename: str = 'контракты.xlsx'):

        try:
            df = pd.DataFrame(data)
            df.to_excel(filename, index=False, engine='openpyxl')
            logger.info(f"Данные сохранены в файл: {filename}")
        except Exception as e:
            logger.error(f"Ошибка при сохранении в Excel: {e}")


def main():

    parser = ContParser()

    logger.info("Начинаем парсинг")
    contracts_data = parser.parse_contracts()

    if contracts_data:
        parser.save_to_excel(contracts_data)
        logger.info("Парсинг завершен успешно")
    else:
        logger.warning("Не удалось получить данные контрактов")


if __name__ == "__main__":
    main()