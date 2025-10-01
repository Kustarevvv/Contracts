import pandas as pd
import numpy as np
pd.set_option('display.max_columns', None)

def load_data(data_name, sheet_name):
    data = pd.read_excel(data_name, sheet_name)
    return data
def data_red(data):
    data['ИНН исполнителя'] = data['ИНН исполнителя'].apply(
        lambda x: f"{int(x):.0f}" if pd.notna(x) else ''
    )
    return data

def start_data(data_name, sheet_name):
    data = load_data(data_name, sheet_name)
    data = data_red(data)

    data['Реестровый номер контракта'] = data['Реестровый номер контракта'].astype('int64').astype(str)
    return data
def quantity_all(data):
    count_contracts = len(data['Реестровый номер контракта'].unique())
    sum_contracts = sum(data['Цена контракта'])
    unique_executors = len(data['ИНН исполнителя'].unique())
    print(f"Общее количество контрактов: {count_contracts}")
    print(f"Общая сумма контрактов: {sum_contracts:.2f}")
    print(f"Общее количество исполнителей: {unique_executors}")

def quantity_executors(data):
    group = data.groupby('ИНН исполнителя', as_index=False).agg({
        'Цена контракта': ['sum', 'mean', 'max'],
        'Фактически оплачено': ['sum','mean'],
        'Реестровый номер контракта': 'nunique',
        'Доходы': 'first',
        'Расходы': 'first',
        'Прибыль(убыток)': 'first',
        'Статус контракта': lambda x: (x == 'Исполнение прекращено').sum(),
        'Неустойки (штрафы и пени)': lambda x: (x == 'Да').sum(),
        'Включение в реестр недобросовестных поставщиков': lambda x: int((x == 'да').any())
        })

    group.columns = ['ИНН исполнителя','Сумма контрактов', 'Средняя цена контракта', 'Максимальная цена контракта',
                     'Сумма оплат', 'Средняя сумма оплат', 'Количество контрактов', 'Доходы', 'Расходы', 'Прибыль(убыток)', 'Количество прекращенных контрактов', 'Неустойки (штрафы и пени)', 'Включение в реестр недобросовестных поставщиков']
    return group

def quantity_status(data):
    group = data.groupby('Статус контракта', as_index=False).agg({
        'Цена контракта': 'sum',
        'Фактически оплачено': 'sum',
        'Реестровый номер контракта': 'nunique',
        'ИНН исполнителя': 'nunique'
        })
    return group

def filtred_data(data):
    data_inn = data[['ИНН исполнителя', 'Фактически оплачено']]

    dict = data_inn.groupby('ИНН исполнителя', as_index=False)['Фактически оплачено'].apply(list).to_dict()

    max_dict = {str(inn).split('.')[0]: max(payments) for inn, payments in dict.items()}

    valid_dict = {inn: payments for inn, payments in max_dict.items() if payments > 250000000 * 0.2}

    return valid_dict

def data_preparation(data_name, sheet_name):
    data = start_data(data_name, sheet_name)
    quantity_all(data)
    quantity_executors(data)
    quantity_status(data)
    filtred_data(data)

def data_for_clust(data_name, sheet_name):
    data = start_data(data_name, sheet_name)
    group = quantity_executors(data)
    return group


