import json
import re

import pandas as pd
from jsoncomment import JsonComment
from sqlalchemy import create_engine


# fill jdbc configs
def df_from_mysql(sql, **jdbc_configs):
    if not jdbc_configs:
        host, port, user, password, database = load_from_config("../../../../../conf/jdbc.conf")
    else:
        host = jdbc_configs.get('host', 'localhost')
        port = jdbc_configs.get('port', 3306)
        user = jdbc_configs.get('user', "")
        password = jdbc_configs.get('password', "")
        database = jdbc_configs.get('database', "")

    print(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')
    db_connection = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')

    return pd.read_sql(sql, con=db_connection)


def load_from_config(config_file_path: str):
    with open(config_file_path, 'r') as file:
        configs = JsonComment(json).load(file)
        user = configs['jdbc']['username']
        password = configs['jdbc']['password']
        url = configs['jdbc']['url']
        host, port, database = re.match('jdbc:mysql://(.*?):(.*?)/(.*?)\?.*', url).groups()
        return host, port, user, password, database


if __name__ == '__main__':
    print(df_from_mysql("select * from texera_db.test_tweets"))
