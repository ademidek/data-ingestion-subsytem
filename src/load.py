import pandas as pd
from typing import List, Callable, Tuple
from psycopg2.extras import execute_values as _execute_values
from .repo import get_conn as _get_conn
import logging

logger = logging.getLogger("etl.load")

def build_upsert_sql(table_name: str, cols: list[str], pk_columns: List[str]) -> Tuple[str, str]:
    col_list_sql = ", ".join(cols)
    values_template = "(" + ", ".join(["%s"] * len(cols)) + ")"

    pk_sql = ", ".join(pk_columns)
    update_cols = [c for c in cols if c not in pk_columns]
    set_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

    sql = f"""
        INSERT INTO {table_name} ({col_list_sql})
        VALUES %s
        ON CONFLICT ({pk_sql}) DO UPDATE
        SET {set_clause};
    """
    return sql, values_template

def upsert_dataframe(
    df: pd.DataFrame,
    table_name: str,
    pk_columns: List[str],
    *,
    conn_factory: Callable = _get_conn,
    execute_values_fn: Callable = _execute_values,
) -> None:
    logger.info(f"Loading {len(df)} rows into {table_name} using PK={pk_columns}")

    if df.empty:
        logger.info(f"[upsert_dataframe] No rows to load into {table_name}.")
        return

    df_copy = df.copy().where(pd.notnull(df), None)
    cols = df_copy.columns.tolist()
    records = df_copy.to_numpy().tolist()

    sql, values_template = build_upsert_sql(table_name, cols, pk_columns)

    with conn_factory() as conn, conn.cursor() as cur:
        execute_values_fn(cur, sql, records, template=values_template)
        conn.commit()

def insert_dataframe(
    df: pd.DataFrame,
    table_name: str,
    *,
    conn_factory: Callable = _get_conn,
    execute_values_fn: Callable = _execute_values,
) -> None:
    if df.empty:
        logger.info(f"[insert_dataframe] No rows to insert into {table_name}.")
        return

    df_copy = df.copy().where(pd.notnull(df), None)
    cols = df_copy.columns.tolist()
    records = df_copy.to_numpy().tolist()

    col_list_sql = ", ".join(cols)
    values_template = "(" + ", ".join(["%s"] * len(cols)) + ")"
    sql = f"INSERT INTO {table_name} ({col_list_sql}) VALUES %s;"

    with conn_factory() as conn, conn.cursor() as cur:
        execute_values_fn(cur, sql, records, template=values_template)
        conn.commit()
