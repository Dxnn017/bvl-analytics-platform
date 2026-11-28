"""Publicación transaccional e idempotente de predicciones y auditoría."""
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

PRED = "ml_predicciones_riesgo"
RUN = "ml_corridas_registro"
KEY = ("nemonico", "fecha_corte", "horizonte_dias", "modelo_version")
VERS = ("version_gold_mercado", "version_gold_sab", "version_gold_empresa")
COMPARE = ("hash_features_entrada", *VERS)


def audit(args, run_id, report):
    return dict(run_id=run_id, tipo_ejecucion=args.env, modelo_version=args.version,
                f1_clase_minoritaria=report["modelo"]["f1_alto"],
                f1_baseline_minoritaria=report["baseline"]["f1_alto"],
                versiones_gold_json=json.dumps(dict(zip(VERS, args.gold))), estado="OK",
                fecha_ejecucion_utc=datetime.now(timezone.utc).isoformat())


def local(rows, db, log):
    rows = iter(rows)
    first = next(rows)
    from itertools import chain
    cols = list(first)
    types = {k: "REAL" for k in ("score_riesgo","frecuencia_cotizacion_20d","volatilidad_20d_pct",
                                 "hhi_mercado_fecha", *(f"probabilidad_clase_{i}" for i in range(3)))}
    types.update(clase_predicha="INTEGER", horizonte_dias="INTEGER")
    placeholders = ",".join("?" for _ in cols)
    updates = ",".join(f"{k}=excluded.{k}" for k in cols if k not in KEY)
    where = " AND ".join(f"{k}=?" for k in KEY)
    Path(db).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db) as con:
        con.execute("BEGIN IMMEDIATE")
        defs = ",".join(f"{k} {types.get(k,'TEXT')}" for k in cols)
        con.execute(f"CREATE TABLE IF NOT EXISTS {PRED} ({defs}, PRIMARY KEY ({','.join(KEY)}))")
        n = ins = upd = changes = 0
        lo = hi = first["fecha_corte"]
        for row in chain([first], rows):
            n += 1
            lo, hi = min(lo,row["fecha_corte"]), max(hi,row["fecha_corte"])
            old = con.execute(f"SELECT clase_predicha,{','.join(COMPARE)} FROM {PRED} WHERE {where}",
                              tuple(row[k] for k in KEY)).fetchone()
            if old and old[1:] == tuple(row[k] for k in COMPARE):
                continue
            ins += old is None
            upd += old is not None
            changes += bool(old and old[0] != row["clase_predicha"])
            con.execute(f"INSERT INTO {PRED} ({','.join(cols)}) VALUES ({placeholders}) "
                        f"ON CONFLICT ({','.join(KEY)}) DO UPDATE SET {updates}",tuple(row[k] for k in cols))
        log.update(fecha_corte_min=lo,fecha_corte_max=hi,registros_evaluados=n,registros_insertados=ins,
                   registros_actualizados=upd,cambios_clase_detectados=changes)
        defs = ",".join(f"{k} {('INTEGER' if isinstance(v,int) else 'REAL' if isinstance(v,float) else 'TEXT')}"
                        for k,v in log.items())
        con.execute(f"CREATE TABLE IF NOT EXISTS {RUN} ({defs}, PRIMARY KEY(run_id))")
        con.execute(f"INSERT INTO {RUN} ({','.join(log)}) VALUES ({','.join('?' for _ in log)})",tuple(log.values()))


def persist(preds, args, run_id, report):
    log = audit(args, run_id, report)
    if args.env == "local":
        local((r.asDict() for r in preds.collect()), args.db, log)
        return
    from google.cloud import bigquery
    ds = "bvl-analytics-2026.gold_analytics"
    client = bigquery.Client(project="bvl-analytics-2026",location="southamerica-west1")
    stage = f"{ds}.ml_stage_{run_id}"
    target, registry = f"`{ds}.{PRED}`", f"`{ds}.{RUN}`"
    on = " AND ".join(f"t.{k}=s.{k}" for k in KEY)
    diff = " OR ".join(f"t.{k} IS DISTINCT FROM s.{k}" for k in COMPARE)
    sets = ",".join(f"{k}=s.{k}" for k in preds.columns if k not in KEY)
    try:
        preds.write.format("bigquery").option("writeMethod","direct").option("table",stage).save()
        params = [bigquery.ScalarQueryParameter(k, "FLOAT64" if isinstance(v,float) else
                  "TIMESTAMP" if k=="fecha_ejecucion_utc" else "STRING", v) for k,v in log.items()]
        cols = ",".join(preds.columns)
        sql = f"""
        BEGIN TRANSACTION;
        INSERT INTO {registry} ({','.join(log)},fecha_corte_min,fecha_corte_max,registros_evaluados,
          registros_insertados,registros_actualizados,cambios_clase_detectados)
        SELECT {','.join('@'+k for k in log)},MIN(s.fecha_corte),MAX(s.fecha_corte),COUNT(*),
          COUNTIF(t.nemonico IS NULL),COUNTIF(t.nemonico IS NOT NULL AND ({diff})),
          COUNTIF(t.clase_predicha != s.clase_predicha AND ({diff}))
        FROM `{stage}` s LEFT JOIN {target} t ON {on};
        MERGE {target} t USING `{stage}` s ON {on}
        WHEN MATCHED AND ({diff}) THEN UPDATE SET {sets}
        WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({','.join('s.'+c for c in preds.columns)});
        COMMIT TRANSACTION;
        """
        client.query(sql,job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
    finally:
        client.delete_table(stage, not_found_ok=True)
