import pandas as pd
import pymysql

class MySQLIOManager:
    def __init__(self, config):
        self._config = config

    def extract_data(self, sql: str) -> pd.DataFrame:
        print("my sql config: ", self._config)
        db_connection = pymysql.connect(
            host='localhost',
            database='youtube',
            user='admin',
            password='admin123',
            port=3307,
        )
        df = pd.read_sql_query(sql=sql, con=db_connection)
        db_connection.close()
        return df
