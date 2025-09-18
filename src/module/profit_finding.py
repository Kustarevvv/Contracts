import os
import xmltodict
import pandas as pd
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

directory = r"C:\fns_xml"
xml_files = [f for f in os.listdir(directory) if f.endswith('.xml')]

df = pd.read_excel('контракты_клининг.xlsx')
inn = df['ИНН исполнителя'].dropna().astype(int).astype(str).unique()

inn_dict = {i: {'Доходы': None, 'Расходы': None, 'Файл': None, 'Дата': None} for i in inn}

for xml_file in xml_files:
    file_path = os.path.join(directory, xml_file)

    try:
        logger.info(f"Обрабатываем файл: {xml_file}")

        with open(file_path, 'r', encoding='utf-8') as file:
            xml_content = file.read()
            data = xmltodict.parse(xml_content)

            documents = data['Файл']['Документ']

            if isinstance(documents, dict):
                documents = [documents]

            for doc in documents:
                inn_val = doc['СведНП']['@ИННЮЛ']

                if inn_val in inn_dict:

                    inn_dict[inn_val]['Доходы'] = doc['СведДохРасх']['@СумДоход']
                    inn_dict[inn_val]['Расходы'] = doc['СведДохРасх']['@СумРасход']
                    inn_dict[inn_val]['Файл'] = xml_file
                    inn_dict[inn_val]['Дата'] = doc['@ДатаДок']

                    logger.info(f"  Найден ИНН: {inn_val}")
                    break

    except Exception as e:
        logger.error(f"Ошибка при обработке файла {xml_file}: {e}")

result_data = []
for inn_val, data in inn_dict.items():
    result_data.append({
        'ИНН': inn_val,
        'Доходы': data['Доходы'],
        'Расходы': data['Расходы']
    })

result_df = pd.DataFrame(result_data)

output_filename = f'результаты_поиска_доходов_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
result_df.to_excel(output_filename, index=False)