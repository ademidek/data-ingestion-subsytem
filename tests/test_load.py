import pandas as pd
from src import load

class FakeCursor:
    def __init__(self):
        self.calls = []
    def __enter__(self): return self
    def __exit__(self, exc_type, exc, tb): return False

class FakeConn:
    def __init__(self, cur):
        self._cur = cur
        self.committed = False
    def cursor(self): return self._cur
    def commit(self): self.committed = True
    def __enter__(self): return self
    def __exit__(self, exc_type, exc, tb): return False

def test_build_upsert_sql_contains_conflict_and_set_clause():
    sql, tmpl = load.build_upsert_sql("stg_esophageal", ["patient_barcode", "gender"], ["patient_barcode"])
    assert "ON CONFLICT (patient_barcode)" in sql
    assert "gender = EXCLUDED.gender" in sql
    assert tmpl.count("%s") == 2

def test_upsert_dataframe_skips_empty_df():
    df = pd.DataFrame(columns=["patient_barcode"])
    called = {"n": 0}
    def fake_conn_factory(): raise AssertionError("should not connect")
    def fake_execute(*args, **kwargs): called["n"] += 1
    load.upsert_dataframe(df, "t", ["patient_barcode"], conn_factory=fake_conn_factory, execute_values_fn=fake_execute)
    assert called["n"] == 0

def test_upsert_dataframe_calls_execute_values_and_commits():
    df = pd.DataFrame([{"patient_barcode": "p1", "gender": "male"}])
    cur = FakeCursor()
    conn = FakeConn(cur)

    def fake_conn_factory(): return conn
    seen = {}
    def fake_execute_values_fn(cursor, sql, records, template):
        seen["sql"] = sql
        seen["records"] = records
        seen["template"] = template

    load.upsert_dataframe(df, "t", ["patient_barcode"], conn_factory=fake_conn_factory, execute_values_fn=fake_execute_values_fn)
    assert "ON CONFLICT" in seen["sql"]
    assert seen["records"] == [["p1", "male"]]
    assert conn.committed is True
