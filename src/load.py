import pandas as pd
from typing import List
import psycopg2
from psycopg2.extras import execute_values
from repo import get_conn
import logging

logger = logging.getLogger("etl.load")

def upsert_dataframe(df, table_name: str, pk_columns: List[str]):

    logger.info(f"Loading {len(df)} rows into {table_name} using PK={pk_columns}")

    if df.empty:
        logger.info(f"[upsert_dataframe] No rows to load into {table_name}.")
        return
    
    df_copy = df.copy()

    df_copy = df_copy.where(pd.notnull(df_copy), None)

    cols = df_copy.columns.tolist()
    records = df_copy.to_numpy().tolist()

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

    with get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, sql, records, template=values_template)
        conn.commit()

    logger.info(f"[upsert_dataframe] Upserted {len(df)} rows into {table_name}.")

def insert_dataframe(df, table_name: str):

    if df.empty:
        logger.info(f"[insert_dataframe] No rows to insert into {table_name}.")
        return
    
    df_copy = df.copy()
    df_copy = df_copy.where(pd.notnull(df_copy), None)

    cols = df_copy.columns.tolist()
    records = df_copy.to_numpy().tolist()

    col_list_sql = ", ".join(cols)
    values_template = "(" + ", ".join(["%s"] * len(cols)) + ")"

    sql = f"""
        INSERT INTO {table_name} ({col_list_sql})
        VALUES %s;
    """

    with get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, sql, records, template=values_template)
        conn.commit()

    logger.info(f"[insert_dataframe] Inserted {len(df)} rows into {table_name}.")
