#!/usr/bin/env python3
import json
import re
import hashlib
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

OUT = Path("outputs/quality/incremental")
OFICIAL = "/datalake/bvl/silver/macroeconomia/tipo_cambio"

def comando(*args):
    p = subprocess.run(
        list(args), capture_output=True, text=True, timeout=180
    )
    if p.returncode:
        raise RuntimeError(p.stderr[-400:])
    return p.stdout

def existe(ruta):
    return subprocess.run(
        ["hdfs", "dfs", "-test", "-e", ruta],
        capture_output=True
    ).returncode == 0

def leer_json(ruta):
    return json.loads(ruta.read_text(encoding="utf-8"))

def huellas(raiz):
    resultado = {}
    listado = comando("hdfs", "dfs", "-ls", "-R", raiz)

    for linea in listado.splitlines():
        partes = linea.split()
        if len(partes) < 8 or not partes[0].startswith("-"):
            continue

        archivo = partes[-1]
        if not archivo.endswith(".parquet"):
            continue

        h = hashlib.sha256()
        proceso = subprocess.Popen(
            ["hdfs", "dfs", "-cat", archivo],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL
        )

        for bloque in iter(
            lambda: proceso.stdout.read(1024 * 1024), b""
        ):
            h.update(bloque)

        if proceso.wait() != 0:
            raise RuntimeError("ERROR_SHA256")

        resultado[archivo[len(raiz):]] = h.hexdigest()

    if not resultado:
        raise RuntimeError("SIN_ARCHIVOS_PARQUET")

    return resultado

registros = sorted(
    OUT.glob("registro_publicacion_*.json"),
    key=lambda p: p.stat().st_mtime,
    reverse=True
)

if not registros:
    raise SystemExit("ERROR|SIN_REGISTRO")

registro = leer_json(registros[0])
run = registro["run_id"]

if not re.fullmatch(r"\d{8}T\d{6}Z_[0-9a-f]{8}", run):
    raise SystemExit("ERROR|RUN_ID_INVALIDO")

base = (
    "/datalake/bvl/staging/"
    f"prueba_publicacion_bcrp_persistente_{run}"
)

anterior = f"{base}/versions/v_{run}_base"
nueva = f"{base}/versions/v_{run}_full"

delta = leer_json(OUT / f"delta_bcrp_v01_{run}.json")
snapshot = leer_json(OUT / f"snapshot_bcrp_full_v01_{run}.json")

salida = OUT / f"manifest_bcrp_v01_{run}.json"
registro_salida = OUT / f"versiones_bcrp_v01_{run}.json"

if salida.exists() or registro_salida.exists():
    raise SystemExit("ERROR|MANIFEST_YA_EXISTE")

if (
    registro.get("tipo") != "LABORATORIO_AISLADO"
    or registro.get("publicacion_aprobada") is not False
    or registro.get("activar_gold") is not False
    or snapshot.get("estado") != "SNAPSHOT_LABORATORIO_VALIDADO"
    or snapshot.get("run_id") != run
    or snapshot.get("ruta_version") != nueva
    or snapshot.get("registros_antes") != 7751
    or snapshot.get("registros_despues") != 7752
    or snapshot.get("publicacion_aprobada") is not False
    or delta.get("run_id") != run
    or delta.get("registros_nuevos") != 1
    or delta.get("registros_corregidos") != 0
    or delta.get("registros_eliminados") != 0
    or delta.get("particiones_afectadas") !=
       [{"anio": 2026, "mes": 9}]
):
    raise SystemExit("ERROR|EVIDENCIAS_INCOMPATIBLES")

cambios = delta["claves_afectadas"]

if (
    len(cambios) != 1
    or cambios[0].get("operacion") != "INSERT"
    or cambios[0].get("clave_anterior") is not None
):
    raise SystemExit("ERROR|DELTA_NO_COINCIDE")

fecha_insertada = cambios[0]["clave_nueva"]["fecha"]

if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", fecha_insertada):
    raise SystemExit("ERROR|FECHA_INVALIDA")

if existe(anterior):
    raise SystemExit("ERROR|BASE_EXISTENTE_NO_SOBRESCRIBIR")

if not existe(OFICIAL) or not existe(nueva):
    raise SystemExit("ERROR|FUENTES_NO_DISPONIBLES")

# Conservar una copia completa de la version anterior.
comando("hdfs", "dfs", "-cp", OFICIAL, anterior)
print("VERSION_ANTERIOR|COPIADA", flush=True)

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("BVL-Manifest-Silver-Gold-v01")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

try:
    original = spark.read.parquet(OFICIAL)
    viejo = spark.read.parquet(anterior)
    nuevo = spark.read.parquet(nueva)

    columnas = sorted(viejo.columns)

    if set(columnas) != set(nuevo.columns):
        raise RuntimeError("ESQUEMAS_DIFERENTES")

    if viejo.schema != nuevo.schema:
        raise RuntimeError("TIPOS_DIFERENTES")

    if original.schema != viejo.schema:
        raise RuntimeError("BASE_INCOMPATIBLE")

    n_antes = viejo.count()
    n_despues = nuevo.count()

    base_diferente = (
        original.select(*columnas)
        .exceptAll(viejo.select(*columnas)).count()
        + viejo.select(*columnas)
        .exceptAll(original.select(*columnas)).count()
    )

    perdidos = (
        viejo.select(*columnas)
        .exceptAll(nuevo.select(*columnas)).count()
    )

    agregados = (
        nuevo.select(*columnas)
        .exceptAll(viejo.select(*columnas)).count()
    )

    fecha_anterior = viejo.filter(
        F.col("fecha").cast("string") == fecha_insertada
    ).count()

    fecha_nueva = nuevo.filter(
        F.col("fecha").cast("string") == fecha_insertada
    ).count()

    if (
        n_antes != 7751
        or n_despues != 7752
        or base_diferente != 0
        or perdidos != 0
        or agregados != 1
        or fecha_anterior != 0
        or fecha_nueva != 1
    ):
        raise RuntimeError("COMPARACION_INVALIDA")

    esquema = hashlib.sha256(
        viejo.schema.json().encode("utf-8")
    ).hexdigest()[:16]

finally:
    spark.stop()

print("COMPARACION_SPARK|APROBADA", flush=True)

firmas_anterior = huellas(anterior)
firmas_nueva = huellas(nueva)

version_anterior = f"v_{run}_base"
version_publicada = f"v_{run}_full"

manifest = {
    "version_manifest": "0.1",
    "entorno": "PRUEBA",
    "run_id": run,
    "estado": "SILVER_VERSION_LABORATORIO_VERIFICADA",
    "fecha_publicacion": datetime.now(
        timezone.utc
    ).isoformat(),
    "tipo_publicacion": "INCREMENTAL",
    "validacion_aprobada": False,
    "datasets": [{
        "dataset": "tipo_cambio",
        "ruta_silver": nueva,
        "version_esquema": f"sha256_{esquema}",
        "version_anterior": version_anterior,
        "version_publicada": version_publicada,
        "clave": ["fecha"],
        "particiones_afectadas": delta["particiones_afectadas"],
        "claves_afectadas": cambios,
        "ruta_detalle_cambios": None,
        "registros_nuevos": 1,
        "registros_corregidos": 0,
        "registros_eliminados": 0,
        "registros_antes": n_antes,
        "registros_despues": n_despues,
        "autorizacion_eliminaciones": False
    }]
}

versiones = {
    "run_id": run,
    "entorno": "PRUEBA",
    "version_anterior": {
        "id": version_anterior,
        "ruta": anterior,
        "archivos_sha256": firmas_anterior
    },
    "version_publicada": {
        "id": version_publicada,
        "ruta": nueva,
        "archivos_sha256": firmas_nueva
    },
    "activar_gold": False,
    "publicacion_oficial": False
}

salida.write_text(
    json.dumps(manifest, indent=2, ensure_ascii=False),
    encoding="utf-8"
)
registro_salida.write_text(
    json.dumps(versiones, indent=2, ensure_ascii=False),
    encoding="utf-8"
)

print(f"REGISTROS_ANTES|{n_antes}")
print(f"REGISTROS_DESPUES|{n_despues}")
print(f"FECHA_INSERTADA|{fecha_insertada}")
print(f"ARCHIVOS_BASE|{len(firmas_anterior)}")
print(f"ARCHIVOS_NUEVOS|{len(firmas_nueva)}")
print("SHA256|REGISTRADO")
print("MANIFEST_V01|GENERADO")
print(f"ARCHIVO|{salida}")
print(f"REGISTRO_VERSIONES|{registro_salida}")
print("PUBLICACION_OFICIAL|NO_EJECUTADA")
print("GOLD|NO_ACTIVADO")
