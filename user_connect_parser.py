#!/usr/bin/python
# -*- coding: utf-8 -*-

import pandas as pd
import os
import re
from datetime import datetime as dttm
from config import *


def find_need_log(path, pattern):
    """
    Функция поиска нужного лог-файла

    :param path: Путь до файла
    :param pattern: Шаблон названия файла
    :return: Название файла
    """
    files = os.listdir(path)
    pattern = dttm.strftime(dttm.now(), pattern)
    for file in files:
        if re.search(pattern, file):
            return file


def obj_spaw_parser(path, log_name):
    """
    Функция парсит лог ObjectSpawner

    :param path: Путь до лога
    :param log_name: Название лога, который будет парситься
    :return: Словарь с последними авторизациями пользователей {user:last_connect}
    """
    with open(str(path) + str(log_name), 'r') as obj_spaw_log:

        connections = {}
        rows = obj_spaw_log.readlines()

        for row in rows:
            if re.search(" - New client connection", row):
                user = row.split()[3][1:]
                user = user.lower().split('@')[0]
                time = dttm.strptime(row.split()[0], '%Y-%m-%dT%H:%M:%S,%f')
                if user in connections:
                    if connections[user] < time:
                        connections[user] = time
                else:
                    connections[user] = time

    return connections


def dict_to_dataframe(dict_users):
    """
    Функция преобразует словарь в DataFrame Pandas

    :param dict_users: Преобразовываемый словарь
    :return: DataFrame
    """
    return pd.DataFrame([[k, v] for k, v in dict_users.items()], columns=['Users', 'Last connect time'])


def dataframe_to_csv(df, file_name, tmp=False):
    """
    Функция выгрузки DataFrame в *.csv

    :param file_name: Название выходного файла
    :param df: DataFrame
    :param tmp: Флаг промежуточного файла
    """
    if tmp:
        result_name = f'intermediate_csv/{file_name}{dttm.strftime(dttm.now(), "%y_%m_%d__%H_%M")}.csv'
    else:
        result_name = file_name
    df.to_csv(result_name, header=True, index=False, sep=';',
              encoding='utf-8', date_format='%Y-%m-%d %H:%M:%S')


def list_files_csv(path, prefix):
    """
    Функция выводит список csv-файлов, в указанной директории

    :param path: Путь до файлов
    :param prefix: Префикс файлов
    :return: Список путей до файлов
    """
    files = os.listdir(path)
    files_csv = []
    for file in files:
        if prefix == file[:13] and file[-3:] == 'csv':
            files_csv.append(file)
    return files_csv


def join_dataframes(files: list[str]):
    """
    Функция объединения нескольких csv-файлов в один DataFrame

    :param files: Список путей до csv-файлов
    :return: DataFrame с объединёнными данными
    """
    return pd.concat([pd.read_csv(f'{INTERMEDIATE_PATH}{file}', sep=';') for file in files], ignore_index=True)


def uniq_data(df: pd.DataFrame):
    """
    Функция удаляет дубли данных, оставляя самые свежие данные

    :param df: DataFrame с дублирующимися строками
    :return: DataFrame с уникальными строками
    """
    return df.drop_duplicates(subset='Users', keep='last')


if __name__ == '__main__':
    # Ищем нужный файл
    file_name = find_need_log(path=LOG_PATH, pattern=LOG_NAME_PATTERN)

    # Собираем данные с лога
    log_data = obj_spaw_parser(path=LOG_PATH, log_name=file_name)
    data = dict_to_dataframe(dict_users=log_data)

    # Выгружаем промежуточные csv-файлы
    dataframe_to_csv(df=data, file_name=CSV_PREFIX, tmp=True)

    # Унифицируем данные
    data = uniq_data(df=join_dataframes(files=list_files_csv(path=INTERMEDIATE_PATH, prefix=CSV_PREFIX)))

    # Выгружаем итоговый csv-файл
    dataframe_to_csv(df=data, file_name=CSV_NAME)
