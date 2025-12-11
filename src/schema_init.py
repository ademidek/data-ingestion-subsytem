from pathlib import Path
from repo import get_conn
import logging

def run_schema():
    logger = logging.getLogger("etl.schema_init")

    root_dir = Path(__file__).resolve().parent
    schema_path = root_dir / "schema.sql"

    if not schema_path.exists():
        raise FileNotFoundError(f"schema.sql not found at {schema_path}")

    sql = schema_path.read_text()

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()

    logger.info(f"[schema_init] Applied schema from {schema_path}")
