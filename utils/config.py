import os
from dotenv import load_dotenv

from configparser import ConfigParser

load_dotenv()

KAGGLE_API_KEY = os.getenv('KAGGLE_DATASET_API_KEY')

# psycopg2 only
def load_config(filename='database.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)
    # get section, default to postgresql
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return config

if __name__ == '__main__':
    config = load_config()
    print(config)