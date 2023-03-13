import pandas as pd
import os
import re
from datetime import datetime


def obj_spaw_parser(log_name: str) -> dict:
    """
    Функция парсит лог ObjectSpawner

    :param log_name: Название лога, который будет парситься
    :return: Словарь с последними авторизациями пользователей {user:last_connect}
    """

    with open(log_name, 'r') as obj_spaw_log:

        connections = {}
        rows = obj_spaw_log.readlines()

        for row in rows:
            if re.search(" - New client connection", row):
                user = row.split()[3][1:]
                user = user.lower().split('@')[0]
                time = datetime.strptime(row.split()[0], '%Y-%m-%dT%H:%M:%S,%f')
                if user in connections:
                    if connections[user] < time:
                        connections[user] = time
                else:
                    connections[user] = time

    return connections


def dict_to_dataframe(dict_users: dict) -> pd.DataFrame:
    """
    Функция преобразует словрь в DataFrame Pandas

    :param dict_users: Преобразовываемый словарь
    :return: DataFrame
    """
    return pd.DataFrame([[k, v] for k, v in dict_users.items()], columns=['Users', 'Last connect time'])


def dataframe_to_csv(df: pd.DataFrame, file_name: str, prefix='') -> None:
    """
    Функция выгрузки DataFrame в *.csv

    :param file_name:
    :param df: DataFrame
    :param prefix: Префикс создаваемого файла
    """
    if prefix != '':
        file_name = f'{datetime.strftime(datetime.now(), f"{prefix}%y_%m_%d__%H_%M.csv")}'
    df.to_csv(file_name, header=True, index=False, sep=';', encoding='utf-8', date_format='%Y-%m-%d %H:%M:%S')


def list_files_csv(prefix: str) -> list:
    files = os.listdir()
    files_csv = []
    for file in files:
        if prefix == file[:13] and file[-3:] == 'csv':
            files_csv.append(file)
    return files_csv


def join_dataframes(files: list[str]) -> pd.DataFrame:
    return pd.concat([pd.read_csv(file, sep=';') for file in files], ignore_index=True)


def uniq_data(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates(subset='Users', keep='last')


if __name__ == '__main__':
    pref = 'user_parser__'
    log_data = obj_spaw_parser('ObjectSpawner.txt')
    data = dict_to_dataframe(log_data)
    dataframe_to_csv(df=data, file_name='', prefix=pref)
    data = uniq_data(join_dataframes(list_files_csv(pref)))
    dataframe_to_csv(df=data, file_name='result.csv')
