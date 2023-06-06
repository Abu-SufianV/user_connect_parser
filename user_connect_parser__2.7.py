import pandas as pd
import os
import re
from config import *


def find_need_log(path, pattern):
    """
    Функция поиска нужного лог-файла

    :param path: Путь до файла
    :param pattern: Шаблон названия файла
    :return: Название файла
    """
    files = os.listdir(path)
    pattern = datetime.now().strftime(pattern)
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
    full_path = os.path.join(path, log_name)
    with open(full_path, 'r') as obj_spaw_log:
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


def dict_to_dataframe(dict_users):
    """
    Функция преобразует словарь в DataFrame Pandas

    :param dict_users: Преобразовываемый словарь
    :return: DataFrame
    """
    return pd.DataFrame(dict_users.items(), columns=['Users', 'Last connect time'])


def dataframe_to_csv(df, file_name, tmp=False):
    """
    Функция выгрузки DataFrame в *.csv

    :param file_name: Название выходного файла
    :param df: DataFrame
    :param tmp: Флаг промежуточного файла
    :return: Имя выгруженного файла
    """
    if tmp:
        result_name = '{}/{}_{}.csv'.format(INTERMEDIATE_PATH, file_name, datetime.now().strftime('%y_%m_%d__%H_%M'))
    else:
        result_name = file_name
    df.to_csv(result_name, header=True, index=False, sep=';',
              encoding='utf-8', date_format='%Y-%m-%d %H:%M:%S')
    return result_name


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


def join_dataframes(files):
    """
    Функция объединения нескольких csv-файлов в один DataFrame

    :param files: Список путей до csv-файлов
    :return: DataFrame с объединёнными данными
    """
    dfs = []
    for file in files:
        file_path = os.path.join(INTERMEDIATE_PATH, file)
        df = pd.read_csv(file_path, sep=';')
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


def uniq_data(df):
    """
    Функция удаляет дубли данных, оставляя самые свежие данные

    :param df: DataFrame с дублирующимися строками
    :return: DataFrame с уникальными строками
    """
    return df.drop_duplicates(subset='Users', keep='last')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    try:
        logging.info("Процесс анализа ObjectSpawner — запущен")

        try:
            logging.info("Поиск нужного файла")
            obj_spw_log = find_need_log(path=LOG_PATH, pattern=LOG_NAME_PATTERN)
            logging.info("Файл: {}".format(obj_spw_log))
        except Exception as er:
            logging.error("Ошибка при поиске нужного файла: {}".format(er))
            raise Exception("Проблема в функции find_need_log()")

        try:
            logging.info("Собираем данные с ObjectSpawner")
            log_data = obj_spaw_parser(path=LOG_PATH, log_name=obj_spw_log)
            data = dict_to_dataframe(dict_users=log_data)
            logging.info("Полученные данные:\n{}".format(data))
        except Exception as er:
            logging.error("Ошибка при сборе данных: {}".format(er))
            raise Exception("Проблема в функциях 'obj_spaw_parser()', 'dict_to_dataframe'")

        try:
            logging.info("Выгружаем промежуточный csv-файл")
            tmp_file = dataframe_to_csv(df=data, file_name=CSV_PREFIX, tmp=True)
            logging.info("Данные выгружены корректно в файл '{}'".format(tmp_file))
        except Exception as er:
            logging.error("Ошибка при выгрузке в файл: {}".format(er))
            raise Exception("Проблема в функции 'dataframe_to_csv()'")

        try:
            logging.info("Унифицируем данные из всех csv-файлов")
            data = uniq_data(df=join_dataframes(files=list_files_csv(path=INTERMEDIATE_PATH, prefix=CSV_PREFIX)))
            logging.info("Данные уникальны")
        except Exception as er:
            logging.error("Ошибка при унификации данных: {}".format(er))
            raise Exception("Проблема в функциях 'uniq_data()', 'join_dataframes()', 'list_files_csv()'")

        # Выгружаем итоговый csv-файл
        try:
            logging.info("Выгружаем {}".format(CSV_NAME))
            dataframe_to_csv(df=data, file_name=CSV_NAME)
            logging.info("Файл {} выгружен успешно".format(CSV_NAME))
        except Exception as er:
            logging.error("Ошибка при выгрузке {}: {}".format(CSV_NAME, er))
            raise Exception("Проблема в функции 'dataframe_to_csv()'")

        logging.info("Процесс анализа ObjectSpawner окончен без ошибок!")
        logging.info("-------------------------------------------------")
    except Exception as er:
        logging.error("Процесс анализа окончен с ошибками: {}".format(er))
        logging.info("------------------------------------------")
