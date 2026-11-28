import csv
import sqlite3

from streamlit.testing.v1 import AppTest

from scripts.ingestion.muestra import TABLES, generate
from scripts.integration.registro import KEY, PRED, RUN, local
from scripts.processing.riesgo import metrics
from pyspark.sql import Row


def test_gold(tmp_path):
    generate(tmp_path, 1)
    def read(table):
        with (tmp_path / f"{table}.csv").open() as f:
            return list(csv.DictReader(f))
    first = read(TABLES[0])
    generate(tmp_path, 2)
    second = read(TABLES[0])
    a = {(r["nemonico"], r["fecha"]): r for r in first}
    b = {(r["nemonico"], r["fecha"]): r for r in second}
    assert len(b) == len(second) and b.keys() > a.keys()
    assert sum(a[k] != b[k] for k in a) == 1
    with open("config/diccionario_gold.csv") as f:
        dictionary = list(csv.DictReader(f))
    for t in TABLES:
        assert set(read(t)[0]) <= {r["columna"] for r in dictionary if r["producto"] == t}


def test_f1():
    rows = [Row(label=2, prediction=2, count=2), Row(label=2, prediction=0, count=2),
            Row(label=0, prediction=2, count=1)]
    result = metrics(rows)
    assert result["soporte_alto"] == 4
    assert result["f1_alto"] == 4/7


def test_upsert_app(tmp_path, monkeypatch):
    db = tmp_path / "ml.sqlite"
    monkeypatch.setenv("ML_DB", str(db))
    monkeypatch.setenv("DATA_BACKEND", "sample")
    first = dict(zip(KEY, ["SIM0","2024-05-02",5,"v1"]))
    first.update(clase_predicha=0, score_riesgo=0.1, hhi_mercado_fecha=5200,
                 hash_features_entrada="a", version_gold_mercado="1",version_gold_empresa="1",version_gold_sab="1")
    second = dict(first, clase_predicha=2, score_riesgo=0.9, hash_features_entrada="b")
    def publish(row, run):
        local([row], db, dict(run_id=run, fecha_ejecucion_utc=run))
    publish(first, "1")
    app = AppTest.from_file("../app.py", default_timeout=15).run()
    assert not app.exception and app.metric[1].value == "0"
    publish(second,"2")
    app.run()
    assert not app.exception and app.metric[1].value == "1"
    publish(second,"3")
    with sqlite3.connect(db) as con:
        assert con.execute(f"SELECT COUNT(*) FROM {PRED}").fetchone() == (1,)
        assert con.execute(f"SELECT registros_insertados,registros_actualizados,cambios_clase_detectados "
                           f"FROM {RUN} ORDER BY run_id").fetchall() == [(1,0,0),(0,1,1),(0,0,0)]

