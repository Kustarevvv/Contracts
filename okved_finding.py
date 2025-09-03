import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
import random
from time import sleep
import logging
import json
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
]

PROXY_LIST = [
    # 'http://164.163.42.3:10000',
    # 'http://92.58.181.171:7575',
    # 'http://66.36.234.130:1339',
    # 'http://164.163.42.12:10000'
    'http://223.204.14.241:8080'
]

BAD_PROXIES = set()
SLOW_PROXIES = set()
USED_PROXIES = set()


def save_progress(inn_dict, filename=None):

    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f'результаты_оквед_промежуточные_{timestamp}.xlsx'

    completed_data = {inn: okved for inn, okved in inn_dict.items() if okved is not None}

    if completed_data:
        result_df = pd.DataFrame(list(completed_data.items()), columns=['ИНН', 'ОКВЭД'])
        result_df.to_excel(filename, index=False)
        logger.info(f"Сохранен промежуточный результат: {filename}")
        logger.info(f"Обработано ИНН: {len(completed_data)}/{len(inn_dict)}")
    else:
        logger.info("Нет данных для сохранения")


def get_random_proxy():

    working_proxies = [p for p in PROXY_LIST if p not in BAD_PROXIES]

    if not working_proxies:
        logger.info("Все прокси нерабочие. Переходим к прямому подключению")
        return None

    proxy_url = random.choice(working_proxies)
    return {
        'http': proxy_url
    }


def update_user_agent(session):

    new_user_agent = random.choice(user_agents)
    session.headers.update({
        'User-Agent': new_user_agent,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    })
    return session


session = requests.Session()
session = update_user_agent(session)

# Основной код
base_url = "https://egrul.itsoft.ru"
df = pd.read_excel('контракты.xlsx')
inn = df['ИНН исполнителя'].dropna().astype(int).astype(str).unique()

inn_dict = {i: None for i in inn}
total_inn = len(inn)
processed_count = 0

logger.info(f"Всего ИНН для обработки: {total_inn}")

try:
    for i in inn_dict:
        logger.info(f"\nОбрабатывается ИНН: {i} ({processed_count + 1}/{total_inn})")

        max_attempts = 4
        attempt = 0
        success = False

        while attempt < max_attempts and not success:
            attempt += 1

            try:
                session = update_user_agent(session)
                proxies = get_random_proxy()

                if proxies:
                    logger.info(f"Попытка {attempt}: Прокси {proxies['http']}")
                    response = session.get(f"{base_url}/{i}.json", timeout=15, proxies=proxies)
                else:
                    logger.info(f"Попытка {attempt}: Прямое соединение")
                    response = session.get(f"{base_url}/{i}.json", timeout=15)

                response.raise_for_status()
                data = response.json()

                if 'СвЮЛ' in data:
                    okved_name = data['СвЮЛ']['СвОКВЭД']['СвОКВЭДОсн']['@attributes']['НаимОКВЭД']
                elif 'СвИП' in data:
                    okved_name = data['СвИП']['СвОКВЭД']['СвОКВЭДОсн']['@attributes']['НаимОКВЭД']
                else:
                    okved_name = "Не найдено"

                inn_dict[i] = okved_name
                logger.info(f"✅ Успешно: {okved_name}")
                success = True
                processed_count += 1

            except requests.exceptions.ProxyError as e:
                logger.error(f"❌ Ошибка прокси: {e}")
                if proxies and 'http' in proxies:
                    BAD_PROXIES.add(proxies['http'])
                sleep(2)

            except requests.exceptions.RequestException as e:
                logger.error(f"Ошибка запроса: {e}")
                sleep(2)

            except Exception as e:
                logger.error(f"Ошибка: {e}")
                sleep(2)

        if not success:
            logger.error(f"Не удалось обработать ИНН {i}")
            inn_dict[i] = "Ошибка: не удалось получить данные"
            processed_count += 1

        if processed_count % 10 == 0 or not success:
            save_progress(inn_dict)

        sleep_duration = random.uniform(2, 5)
        sleep(sleep_duration)

except KeyboardInterrupt:
    logger.info("\nПрограмма прервана")
    save_progress(inn_dict, 'результаты_оквед_прервано.xlsx')

except Exception as e:
    logger.error(f"\nКритическая ошибка: {e}")
    save_progress(inn_dict, 'результаты_оквед_ошибка.xlsx')

finally:

    print("\n" + "=" * 50)
    logger.info("ЗАВЕРШЕНИЕ РАБОТЫ")
    print("=" * 50)

    save_progress(inn_dict, 'результаты_оквед_финальные.xlsx')

    result_df = pd.DataFrame(list(inn_dict.items()), columns=['ИНН', 'ОКВЭД'])
    result_df.to_excel('результаты_оквед_полные.xlsx', index=False)

    logger.info(f"Обработано {processed_count}/{total_inn} ИНН")
    logger.info("Все файлы сохранены")