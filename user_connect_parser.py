import pandas as pd
import os
import re
import logging
from datetime import datetime as dttm
from config import *


def find_need_log(path: str, pattern: str) -> str:
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


def obj_spaw_parser(path: str, log_name: str) -> dict:
    """
    Функция парсит лог ObjectSpawner

    :param path: Путь до лога
    :param log_name: Название лога, который будет парситься
    :return: Словарь с последними авторизациями пользователей {user:last_connect}
    """
    full_path = str(path) + str(log_name)
    with open(full_path, 'r') as obj_spaw_log:

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


def dict_to_dataframe(dict_users: dict) -> pd.DataFrame:
    """
    Функция преобразует словарь в DataFrame Pandas

    :param dict_users: Преобразовываемый словарь
    :return: DataFrame
    """
    return pd.DataFrame([[k, v] for k, v in dict_users.items()], columns=['Users', 'Last connect time'])


def dataframe_to_csv(df: pd.DataFrame, file_name: str, tmp=False) -> str:
    """
    Функция выгрузки DataFrame в *.csv

    :param file_name: Название выходного файла
    :param df: DataFrame
    :param tmp: Флаг промежуточного файла
    :return: Имя выгруженного файла
    """
    if tmp:
        result_name = f'{INTERMEDIATE_PATH}/{file_name}{dttm.strftime(dttm.now(), "%y_%m_%d__%H_%M")}.csv'
    else:
        result_name = file_name
    df.to_csv(result_name, header=True, index=False, sep=';',
              encoding='utf-8', date_format='%Y-%m-%d %H:%M:%S')
    return result_name


def list_files_csv(path: str, prefix: str) -> list:
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


def join_dataframes(files: list) -> pd.DataFrame:
    """
    Функция объединения нескольких csv-файлов в один DataFrame

    :param files: Список путей до csv-файлов
    :return: DataFrame с объединёнными данными
    """
    return pd.concat([pd.read_csv(f'{INTERMEDIATE_PATH}/{file}', sep=';') for file in files], ignore_index=True)


def uniq_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Функция удаляет дубли данных, оставляя самые свежие данные

    :param df: DataFrame с дублирующимися строками
    :return: DataFrame с уникальными строками
    """
    return df.drop_duplicates(subset='Users', keep='last')


if __name__ == '__main__':
    logging.info("Процесс анализа ObjectSpawner — запущен")
    try:
        logging.info("Поиск нужного файла")
        obj_spw_log = find_need_log(path=LOG_PATH, pattern=LOG_NAME_PATTERN)
        logging.info(f"Файл: {obj_spw_log}")
    except Exception as er:
        logging.error(f"Ошибка при поиске нужного файла: {er}")
        raise "Ошибка при поиске нужного файла"


    try:
        logging.info("Собираем данные с ObjectSpawner")
        log_data = obj_spaw_parser(path=LOG_PATH, log_name=obj_spw_log)
        data = dict_to_dataframe(dict_users=log_data)
        logging.info(f"Полученные данные: \n{data}")
    except Exception as er:
        logging.error(f"Ошибка при сборе данных: {er}")
        raise f"Ошибка при сборе данных: {er}"
    try:

        logging.info("Выгружаем промежуточный csv-файл")
        tmp_file = dataframe_to_csv(df=data, file_name=CSV_PREFIX, tmp=True)
        logging.info(f"Данные выгружены корректно в файл '{tmp_file}'")
    except Exception as er:
        logging.error(f"Ошибка при выгрузке в файл: {er}")
        raise f"Ошибка при выгрузке в файл: {er}"

    try:
        logging.info("Унифицируем данные из всех csv-файлов")
        data = uniq_data(df=join_dataframes(files=list_files_csv(path=INTERMEDIATE_PATH, prefix=CSV_PREFIX)))
        logging.info("Данные уникальны")
    except Exception as er:
        logging.error(f"Ошибка при унификации данных: {er}")
        raise f"Ошибка при унификации данных: {er}"


    # Выгружаем итоговый csv-файл
    try:
        logging.info(f"Выгружаем {CSV_NAME}")
        dataframe_to_csv(df=data, file_name=CSV_NAME)
        logging.info(f"Файл {CSV_NAME} выгружен успешно")
    except Exception as er:
        logging.error(f"Ошибка при выгрузке {CSV_NAME}: {er}")
        raise f"Ошибка при выгрузке {CSV_NAME}: {er}"

    logging.info("Процесс анализа ObjectSpawner — окончен")
