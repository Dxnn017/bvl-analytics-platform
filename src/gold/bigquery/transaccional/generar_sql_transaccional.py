"""Generador local de SQL transaccional candidato.

No importa clientes cloud ni ejecuta SQL.
Conserva los bloqueos operativos originales.
"""

import json
import hashlib
import re

from pathlib import Path

from preparador_incremental import (
    cargar_contratos_incrementales,
)

from src.gold.bigquery.comparador_negocio_local import (
    METADATOS_TECNICOS,
)


PROYECTO = "bvl-analytics-2026"

CONTROL = (
    f"{PROYECTO}.gold_analytics_control"
)

VERSIONES = f"`{CONTROL}.product_versions_v2`"
HISTORIAL = f"`{CONTROL}.version_history_v2`"
BATCHES = f"`{CONTROL}.incremental_batches_v2`"

BLOQUEO = """-- CANDIDATO LOCAL NO AUTORIZADO.
-- No retirar el bloqueo sin acreditacion Quality.
-- No ejecutar directamente en GCP.

ASSERT FALSE AS
  'EJECUCION OPERATIVA NO AUTORIZADA';
"""


def identificador(nombre):

    if not re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_]*",
        nombre,
    ):

        raise ValueError(
            "Identificador SQL incompatible"
        )

    return f"`{nombre}`"


def generar_control():

    return BLOQUEO + f"""
CREATE TABLE IF NOT EXISTS {VERSIONES} (
  product STRING NOT NULL,
  current_version STRING NOT NULL,
  updated_at TIMESTAMP NOT NULL
)
CLUSTER BY product;

CREATE TABLE IF NOT EXISTS {HISTORIAL} (
  product STRING NOT NULL,
  version STRING NOT NULL,
  applied_at TIMESTAMP NOT NULL
)
CLUSTER BY product, version;

CREATE TABLE IF NOT EXISTS {BATCHES} (
  product STRING NOT NULL,
  batch_id STRING NOT NULL,
  run_id STRING NOT NULL,
  source_version STRING NOT NULL,
  previous_version STRING NOT NULL,
  new_version STRING NOT NULL,
  source_manifest_sha256 STRING NOT NULL,
  gold_contract_sha256 STRING NOT NULL,
  payload_sha256 STRING NOT NULL,
  staging_content_sha256 STRING NOT NULL,
  evidence_sha256 STRING NOT NULL,
  transfer_sha256 STRING NOT NULL,
  status STRING NOT NULL,
  received_rows INT64 NOT NULL,
  inserted_rows INT64 NOT NULL,
  updated_rows INT64 NOT NULL,
  business_updates INT64 NOT NULL,
  technical_updates INT64 NOT NULL,
  unchanged_rows INT64 NOT NULL,
  deleted_rows INT64 NOT NULL,
  finished_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(finished_at)
CLUSTER BY product, batch_id;
"""


def diferencia(columnas):

    return " OR ".join(
        f"T.{identificador(c)} IS DISTINCT FROM "
        f"S.{identificador(c)}"
        for c in columnas
    ) or "FALSE"


def validaciones_originales(raiz, producto):

    ruta = (
        raiz / "docs/bigquery"
        / f"{producto}_merge.sql"
    )

    texto = ruta.read_text(encoding="utf-8")

    # Recuperar exclusivamente las validaciones
    # anteriores al MERGE original.

    inicio = re.search(
        r"(?m)^ASSERT\s*\(",
        texto,
    )

    final = re.search(
        r"(?m)^MERGE\s+",
        texto,
    )

    if not inicio or not final:

        raise ValueError(
            "No se encontraron controles originales"
        )

    controles = texto[
        inicio.start():final.start()
    ]

    if "ASSERT FALSE" not in texto:

        raise ValueError(
            "Bloqueo original ausente"
        )

    for dataset in (
        "gold_analytics",
        "gold_analytics_staging",
    ):

        controles = controles.replace(
            f"`{dataset}.",
            f"`{PROYECTO}.{dataset}.",
        )

    return controles


def generar_producto(raiz, producto, contrato):

    campos = contrato["gold"]["columns"]

    columnas = [
        campo["name"]
        for campo in campos
    ]

    claves = contrato["gold"]["business_keys"]

    negocio = [
        c for c in columnas
        if c not in METADATOS_TECNICOS
    ]

    tecnicas = [
        c for c in columnas
        if c in METADATOS_TECNICOS
    ]

    destino = (
        f"`{PROYECTO}.gold_analytics."
        f"{producto}`"
    )

    staging = (
        f"`{PROYECTO}.gold_analytics_staging."
        f"{producto}_delta`"
    )

    nombres = ", ".join(
        identificador(c)
        for c in columnas
    )

    valores = ", ".join(
        f"S.{identificador(c)}"
        for c in columnas
    )

    union = " AND ".join(
        f"T.{identificador(c)} = "
        f"S.{identificador(c)}"
        for c in claves
    )

    asignaciones = ",\n      ".join(
        f"{identificador(c)} = "
        f"S.{identificador(c)}"
        for c in columnas
        if c not in claves
    )

    primera_clave = identificador(
        claves[0]
    )

    distintos = diferencia(columnas)

    diferencia_negocio = diferencia(negocio)
    diferencia_tecnica = diferencia(tecnicas)

    controles = validaciones_originales(
        raiz, producto
    )

    identidad = (
        "run_id",
        "source_version",
        "previous_version",
        "new_version",
        "source_manifest_sha256",
        "gold_contract_sha256",
        "payload_sha256",
        "staging_content_sha256",
        "evidence_sha256",
        "transfer_sha256",
    )

    identidad_incompatible = "\n      OR ".join(
        f"{c} IS DISTINCT FROM @{c}"
        for c in identidad
    )

    hashes = (
        "source_manifest_sha256",
        "gold_contract_sha256",
        "payload_sha256",
        "staging_content_sha256",
        "evidence_sha256",
        "transfer_sha256",
    )

    controles_hash = "\n  AND ".join(
        f"COALESCE(REGEXP_CONTAINS(@{c}, "
        "r'^[0-9a-f]{64}$'), FALSE)"
        for c in hashes
    )

    fecha = identificador(
        contrato["configuracion"][
            "bigquery_partition"
        ]
    )

    periodo = (
        "QUARTER"
        if producto == "empresa_trimestre"
        else "MONTH"
    )

    columna_periodo = (
        "trimestre"
        if producto == "empresa_trimestre"
        else "mes"
    )

    controles_particiones = f"""
ASSERT (
  SELECT COUNT(*) = 0
  FROM delta_batch
  WHERE {fecha} IS NULL
     OR anio IS DISTINCT FROM
        EXTRACT(YEAR FROM {fecha})
     OR {columna_periodo} IS DISTINCT FROM
        EXTRACT({periodo} FROM {fecha})
) AS 'Particiones incompatibles';
"""

    condiciones_numericas = []

    for campo in campos:

        nombre = identificador(
            campo["name"]
        )

        if campo["bigquery_type"] == "FLOAT64":

            condiciones_numericas.append(
                f"IS_NAN({nombre}) OR IS_INF({nombre})"
            )

        if campo["bigquery_type"] == "NUMERIC":

            condiciones_numericas.append(
                f"{nombre} IS DISTINCT FROM "
                f"TRUNC({nombre}, 2)"
            )

    numericos = (
        "\n     OR ".join(
            condiciones_numericas
        )
        or "FALSE"
    )

    return BLOQUEO + f"""
-- Producto: {producto}
-- Requiere reserva exclusiva externa por producto.
-- La identidad criptografica debe acreditarse antes
-- de cargar staging. Comparar parametros no verifica
-- por si solo el contenido de los objetos Parquet.
-- Carga historica oficial previa obligatoria.
-- Ejecutar como un script unico, no por fragmentos.

ASSERT (
  @expected_rows IS NOT NULL
  AND @expected_rows > 0
  AND @batch_id IS NOT NULL
  AND TRIM(@batch_id) != ''
  AND @run_id IS NOT NULL
  AND TRIM(@run_id) != ''
  AND @source_version IS NOT NULL
  AND TRIM(@source_version) != ''
  AND @previous_version IS NOT NULL
  AND TRIM(@previous_version) != ''
  AND @new_version IS NOT NULL
  AND TRIM(@new_version) != ''
  AND @previous_version != @new_version
  AND {controles_hash}
  AND @gold_contract_sha256 =
      '{contrato["sha256"]}'
) AS 'Identidad candidata incompatible';

BEGIN
  BEGIN TRANSACTION;

  ASSERT (
    SELECT COUNT(*) <= 1
    FROM {BATCHES}
    WHERE product = '{producto}'
      AND batch_id = @batch_id
  ) AS 'Batch duplicado';

  ASSERT (
    SELECT COUNT(*) = 0
    FROM {BATCHES}
    WHERE product = '{producto}'
      AND batch_id = @batch_id
      AND (
        {identidad_incompatible}
        OR status IS DISTINCT FROM 'COMPLETED'
        OR received_rows != @expected_rows
        OR updated_rows !=
           business_updates + technical_updates
        OR received_rows !=
           inserted_rows + updated_rows + unchanged_rows
        OR deleted_rows != 0
        OR inserted_rows < 0
        OR business_updates < 0
        OR technical_updates < 0
        OR unchanged_rows < 0
      )
  ) AS 'Identidad o auditoria de replay incompatible';

  IF EXISTS (
    SELECT 1 FROM {BATCHES}
    WHERE product = '{producto}'
      AND batch_id = @batch_id
  ) THEN

    ASSERT (
      SELECT COUNT(*) = 1
      FROM {HISTORIAL}
      WHERE product = '{producto}'
        AND version = @new_version
    ) AS 'Replay sin historial de version';

    SELECT 'REPLAY_NOOP' AS resultado_batch,
           0 AS applied_rows;

  ELSE

    ASSERT (
      SELECT COUNT(*) = 1
      FROM {VERSIONES}
      WHERE product = '{producto}'
    ) AS 'Registro de version ausente o duplicado';

    ASSERT (
      SELECT COUNT(*) = 1
      FROM {VERSIONES}
      WHERE product = '{producto}'
        AND current_version = @previous_version
    ) AS 'Continuidad incompatible';

    ASSERT (
      SELECT COUNT(*) = 1
      FROM {HISTORIAL}
      WHERE product = '{producto}'
        AND version = @previous_version
    ) AS 'Version anterior sin historial';

    ASSERT (
      SELECT COUNT(*) = 0
      FROM {HISTORIAL}
      WHERE product = '{producto}'
        AND version = @new_version
    ) AS 'Version previamente aplicada';

    -- No omitir filas del batch con metadatos distintos.
    ASSERT (
      SELECT COUNT(*) = 0
      FROM {staging}
      WHERE batch_id = @batch_id
        AND (
          run_id IS DISTINCT FROM @run_id
          OR source_version IS DISTINCT FROM @source_version
          OR payload_sha256 IS DISTINCT FROM @payload_sha256
          OR operation IS DISTINCT FROM 'UPSERT'
          OR delete_authorized IS DISTINCT FROM FALSE
        )
    ) AS 'Batch incompatible o DELETE bloqueado';

    CREATE TEMP TABLE delta_batch AS
    SELECT *
    FROM {staging}
    WHERE batch_id = @batch_id;

    ASSERT (
      SELECT COUNT(*) = @expected_rows
      FROM delta_batch
    ) AS 'Conteo staging incompatible';

    -- Controles originales del producto:
    -- claves, nulos, cobertura y operaciones.

{controles}

{controles_particiones}

    ASSERT (
      SELECT COUNT(*) = 0
      FROM delta_batch
      WHERE {numericos}
    ) AS 'Numericos incompatibles';

    -- Clasificacion previa al MERGE.
    CREATE TEMP TABLE metricas_batch AS
    WITH clasificacion AS (
      SELECT CASE
        WHEN T.{primera_clave} IS NULL
          THEN 'INSERT'
        WHEN ({diferencia_negocio})
          THEN 'UPDATE_BUSINESS'
        WHEN ({diferencia_tecnica})
          THEN 'UPDATE_TECHNICAL'
        ELSE 'NOOP'
      END AS accion
      FROM delta_batch S
      LEFT JOIN {destino} T
        ON {union}
    )
    SELECT
      COUNT(*) AS received_rows,
      COUNTIF(accion = 'INSERT') AS inserted_rows,
      COUNTIF(accion IN (
        'UPDATE_BUSINESS',
        'UPDATE_TECHNICAL'
      )) AS updated_rows,
      COUNTIF(accion = 'UPDATE_BUSINESS')
        AS business_updates,
      COUNTIF(accion = 'UPDATE_TECHNICAL')
        AS technical_updates,
      COUNTIF(accion = 'NOOP') AS unchanged_rows,
      CAST(0 AS INT64) AS deleted_rows
    FROM clasificacion;

    ASSERT (
      SELECT
        received_rows = @expected_rows
        AND updated_rows =
            business_updates + technical_updates
        AND received_rows =
            inserted_rows + updated_rows + unchanged_rows
        AND deleted_rows = 0
      FROM metricas_batch
    ) AS 'Metricas incompatibles';

    MERGE {destino} T
    USING delta_batch S
      ON {union}

    WHEN MATCHED
      AND ({distintos})
    THEN UPDATE SET
      {asignaciones}

    WHEN NOT MATCHED
    THEN INSERT ({nombres})
      VALUES ({valores});

    -- Conciliacion exacta de claves afectadas.
    ASSERT (
      SELECT COUNT(*) = 0
      FROM delta_batch S
      LEFT JOIN {destino} T
        ON {union}
      WHERE T.{primera_clave} IS NULL
         OR ({distintos})
    ) AS 'Conciliacion posterior incompatible';

    ASSERT (
      SELECT COUNT(*) = @expected_rows
      FROM delta_batch S
      JOIN {destino} T
        ON {union}
    ) AS 'Cardinalidad posterior incompatible';

    INSERT INTO {BATCHES} (
      product, batch_id, run_id, source_version,
      previous_version, new_version,
      source_manifest_sha256,
      gold_contract_sha256,
      payload_sha256, staging_content_sha256,
      evidence_sha256, transfer_sha256,
      status, received_rows, inserted_rows,
      updated_rows, business_updates,
      technical_updates, unchanged_rows,
      deleted_rows, finished_at
    )
    SELECT
      '{producto}', @batch_id, @run_id,
      @source_version, @previous_version,
      @new_version, @source_manifest_sha256,
      @gold_contract_sha256,
      @payload_sha256, @staging_content_sha256,
      @evidence_sha256, @transfer_sha256,
      'COMPLETED', received_rows, inserted_rows,
      updated_rows, business_updates,
      technical_updates, unchanged_rows,
      deleted_rows, CURRENT_TIMESTAMP()
    FROM metricas_batch;

    UPDATE {VERSIONES}
    SET current_version = @new_version,
        updated_at = CURRENT_TIMESTAMP()
    WHERE product = '{producto}'
      AND current_version = @previous_version;

    ASSERT @@row_count = 1
      AS 'Avance de version incompatible';

    INSERT INTO {HISTORIAL} (
      product, version, applied_at
    )
    VALUES (
      '{producto}', @new_version, CURRENT_TIMESTAMP()
    );

  END IF;

  COMMIT TRANSACTION;

EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  RAISE;
END;
"""


def generar(raiz):

    raiz = Path(raiz).resolve()

    contratos = cargar_contratos_incrementales(
        raiz
    )

    destino = (
        raiz / "docs/bigquery"
        / "integracion_transaccional"
        / "sql_candidato"
    )

    destino.mkdir(
        parents=True,
        exist_ok=False,
    )

    documentos = {
        "control_transaccional_v2.sql":
            generar_control()
    }

    for producto, contrato in contratos.items():

        documentos[
            f"{producto}_transaccional_v2.sql"
        ] = generar_producto(
            raiz,
            producto,
            contrato,
        )

    inventario = []

    for nombre, contenido in sorted(
        documentos.items()
    ):

        ruta = destino / nombre

        with ruta.open(
            "x",
            encoding="utf-8",
        ) as archivo:

            archivo.write(contenido)

        bytes_sql = ruta.read_bytes()

        inventario.append({
            "path": str(
                ruta.relative_to(raiz)
            ),
            "sha256": hashlib.sha256(
                bytes_sql
            ).hexdigest(),
            "size_bytes": len(bytes_sql),
        })

    informe = {
        "status":
            "LOCAL_SQL_CANDIDATE_NOT_AUTHORIZED",
        "base_commit":
            "05d2b78f168b771a2a08a6cedb1b5bc3f3dd41b8",
        "files": inventario,
        "products": sorted(contratos),
        "execution_authorized": False,
        "delete_authorized": False,
        "production_write_authorized": False,
        "publication_authorized": False,
        "google_sql_engine_validated": False,
        "distributed_lock_verified": False,
    }

    with (
        destino / "inventario_sql.json"
    ).open(
        "x",
        encoding="utf-8",
    ) as archivo:

        json.dump(
            informe,
            archivo,
            indent=2,
            ensure_ascii=False,
        )

        archivo.write("\n")

    return informe


if __name__ == "__main__":

    informe = generar(Path.cwd())

    for archivo in informe["files"]:

        print(
            archivo["sha256"],
            archivo["path"],
        )

    print("SQL_CANDIDATOS_GENERADOS: 4")
