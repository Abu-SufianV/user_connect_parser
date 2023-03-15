# Настройки логирования
import logging
from datetime import datetime

log_filename = f"./logs/{datetime.date(datetime.now())}.log".replace("-", "_")
log_format = "%(asctime)s [%(levelname)s]: %(message)s"
log_level = logging.INFO
logging.basicConfig(level=log_level, filename=log_filename, format=log_format)

# Путь до лога и название
LOG_PATH         = "./"
LOG_NAME_PATTERN = "ObjectSpawner.txt"

# Префикс промежуточных csv-файлов и название итогового csv-файла
CSV_PREFIX = "user_parser__"
CSV_NAME   = "result.csv"

# Относительный путь до промежуточных файлов
INTERMEDIATE_PATH = "./intermediate_csv"
