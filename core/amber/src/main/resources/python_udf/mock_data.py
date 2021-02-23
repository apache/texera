import pandas as pd
from sqlalchemy import create_engine


# fill jdbc configs
def df_from_mysql(sql,
                  host="localhost",
                  port=3306,
                  user="",
                  password="",
                  database=""):
    db_connection = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')

    return pd.read_sql(sql, con=db_connection)


if __name__ == '__main__':
    print(df_from_mysql("select * from texera_db.test_tweets"))
