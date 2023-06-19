import csv
import os
import re
from config import *


def find_need_log(path, pattern):
    files = os.listdir(path)
    pattern = datetime.now().strftime(pattern)
    for file in files:
        if re.search(pattern, file):
            return file


def obj_spaw_parser(path, log_name):
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


def dict_to_csv(filename, data):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=';')

        # Записываем заголовки столбцов
        writer.writerow(['Users', 'Last connect time'])

        # Записываем данные
        for user, date in data.items():
            formatted_date = date.strftime('%Y-%m-%d %H:%M:%S')
            writer.writerow([user, formatted_date])


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    try:
        logging.info("Analyse ObjectSpawner — started")

        try:
            logging.info("Find need file")
            obj_spw_log = find_need_log(path=LOG_PATH, pattern=LOG_NAME_PATTERN)
            logging.info("File: {}".format(obj_spw_log))
        except Exception as er:
            logging.error("ERROR in find file: {}".format(er))
            raise Exception("Problem in find_need_log()")

        try:
            logging.info("Gather data from ObjectSpawner")
            log_data = obj_spaw_parser(path=LOG_PATH, log_name=obj_spw_log)
            logging.info("Dict: \n{}".format(log_data))
            dict_to_csv(filename=CSV_NAME, data=log_data)
        except Exception as er:
            logging.error("ERROR in gather: {}".format(er))
            raise Exception("Problem in 'obj_spaw_parser()', 'dict_to_dataframe'")

        logging.info("Process ObjectSpawner finished successfull!")
        logging.info("-------------------------------------------------")
    except Exception as er:
        logging.error("Process ObjectSpawner finished with errors: {}".format(er))
        logging.info("------------------------------------------")
