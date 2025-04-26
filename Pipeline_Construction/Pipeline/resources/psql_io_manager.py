from typing import Any
from dagster._core.execution.context.input import InputContext
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from sqlalchemy import create_engine
from psycopg2 import connect
from contextlib import contextmanager


@contextmanager
def connect_psql(config):
    conn_info = (
    'postgresql://admin:admin123@localhost:5432/youtube'
    )
    db_conn = create_engine(conn_info)
    try:
        yield db_conn
    except Exception:
        raise

class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config
    def load_input(self, context: InputContext) -> pd.DataFrame:
        pass
    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        schema, table = context.asset_key.path[-2], context.asset_key.path[-1]
        with connect_psql(self._config) as conn:
        # insert new data
            ls_columns = (context.metadata or {}).get("columns", [])
            obj[ls_columns].to_sql(
                name=f"{table}",
                con=conn,
                schema=schema,
                if_exists="replace",
                index=False,
                chunksize=10000,
                method="multi"
            )