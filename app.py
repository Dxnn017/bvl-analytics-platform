import os
import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

from scripts.integration.registro import PRED, RUN

DATASET = "bvl-analytics-2026.gold_analytics"


def read_tables():
    backend = os.getenv("DATA_BACKEND", "sample")
    if backend == "sample":
        path = Path(os.getenv("ML_DB", "outputs/riesgo.sqlite")).resolve()
        with sqlite3.connect(path.as_uri()+"?mode=ro", uri=True) as con:
            predictions = pd.read_sql_query(f"SELECT * FROM {PRED}", con)
            runs = pd.read_sql_query(f"SELECT * FROM {RUN} ORDER BY fecha_ejecucion_utc DESC LIMIT 10", con)
        return predictions, runs
    if backend == "bigquery":
        from google.cloud import bigquery
        client = bigquery.Client(project="bvl-analytics-2026", location="southamerica-west1")
        def query(table, suffix):
            result = client.query(f"SELECT * FROM `{DATASET}.{table}` {suffix}").result()
            return pd.DataFrame([dict(row) for row in result], columns=[c.name for c in result.schema])
        return query(PRED, ""), query(RUN, "ORDER BY fecha_ejecucion_utc DESC LIMIT 10")
    raise ValueError("DATA_BACKEND debe ser sample o bigquery")


def main():
    st.set_page_config(page_title="Riesgo BVL", layout="wide")
    st.title("Riesgo por valor · BVL")
    try:
        df, runs = read_tables()
    except Exception as exc:
        st.error(f"No se pudieron leer las predicciones. Comprueba el backend y la última carga. {exc}")
        return
    if df.empty:
        st.info("Aún no hay predicciones disponibles.")
        return
    version = st.selectbox("Versión del modelo", sorted(df.modelo_version.unique()))
    df = df[df.modelo_version == version]
    cutoff = st.selectbox("Fecha de corte", sorted(df.fecha_corte.unique(), reverse=True))
    snapshot = df[df.fecha_corte == cutoff]
    a,b,c = st.columns(3)
    a.metric("Valores evaluados", snapshot.nemonico.nunique())
    b.metric("Valores con riesgo alto", snapshot.loc[snapshot.clase_predicha == 2,"nemonico"].nunique())
    hhi = snapshot.hhi_mercado_fecha.dropna().unique()
    c.metric("HHI del mercado", f"{hhi[0]:,.0f}" if len(hhi)==1 else "Sin dato consistente")
    symbol = st.selectbox("Nemónico", sorted(snapshot.nemonico.unique()))
    series = df[(df.nemonico == symbol) & (df.fecha_corte <= cutoff)].sort_values("fecha_corte")
    st.line_chart(series.set_index("fecha_corte")[["score_riesgo"]])
    st.dataframe(series.sort_values("fecha_corte",ascending=False), hide_index=True)
    st.subheader("Últimas corridas")
    st.dataframe(runs, hide_index=True)


if __name__ == "__main__":
    main()
