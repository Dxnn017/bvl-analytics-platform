"""Recursos candidatos de consumo analitico.

Sin ejecucion SQL ni acceso cloud.
"""

import json
import hashlib

from pathlib import Path

from preparador_carga_historica import (
    cargar_contratos,
    exigir,
)


MERCADO = "mercado_valor_diario"
SAB = "actividad_sab_diaria"
EMPRESA = "empresa_trimestre"

PROYECTO = "bvl-analytics-2026"

DATASET = "gold_analytics"

ESTADO = "LOCAL_CONSUMPTION_CANDIDATE_ONLY"

CABECERA = """-- CANDIDATO LOCAL NO AUTORIZADO.
-- Consulta de referencia, no fuente operativa.
-- No ejecutar sin acreditacion correspondiente.

ASSERT FALSE AS
  'CONSUMO OPERATIVO NO AUTORIZADO';

ASSERT (
  @fecha_inicio IS NOT NULL
  AND @fecha_fin IS NOT NULL
  AND @fecha_inicio <= @fecha_fin
) AS 'Intervalo temporal incompatible';
"""


# Indicadores existentes, no formulas nuevas.
# producto, campo, formula documentada,
# condiciones y tratamiento analitico.

INDICADORES = [

    (
        MERCADO,
        "precio_cierre",
        "Precio de cierre de la fuente bursatil",
        ["precio_cierre_valido"],
        "No sumar entre valores o fechas",
        None,
    ),

    (
        MERCADO,
        "retorno_diario_pct",
        "(precio_cierre / precio_anterior - 1) * 100",
        ["retorno_calculable"],
        "Retorno simple de precio, no retorno total ajustado",
        "porcentaje",
    ),

    (
        MERCADO,
        "rango_intradia_pct",
        "(precio_maximo - precio_minimo) / precio_anterior * 100",
        [
            "rango_intradia_valido",
            "precio_anterior_valido",
        ],
        "No sumar porcentajes",
        "porcentaje",
    ),

    (
        SAB,
        "monto_compras",
        "MontoCompras de Silver con precision decimal",
        ["montos_validos", "total_consistente"],
        "Preservar NUMERIC y verificar cobertura",
        None,
    ),

    (
        SAB,
        "monto_ventas",
        "MontoVentas de Silver con precision decimal",
        ["montos_validos", "total_consistente"],
        "Preservar NUMERIC y verificar cobertura",
        None,
    ),

    (
        SAB,
        "saldo_neto",
        "monto_compras - monto_ventas",
        ["montos_validos"],
        "Preservar signo, precision y cobertura",
        None,
    ),

    (
        SAB,
        "participacion_total_pct",
        "monto_total / total_actividad_diaria * 100",
        ["actividad_diaria_valida"],
        "Participacion sobre universo observado; total positivo",
        "porcentaje",
    ),

    (
        SAB,
        "hhi_diario",
        "SUM(POW(participacion_total_pct, 2)) por fecha",
        ["actividad_diaria_valida"],
        "Valor diario repetido por SAB; no sumar filas",
        "indice HHI de 0 a 10000",
    ),

    (
        EMPRESA,
        "roa_aproximado_pct",
        "utilidad_neta / activo_total * 100",
        ["roa_calculable"],
        "Aproximado; no anualizar automaticamente",
        "porcentaje",
    ),

    (
        EMPRESA,
        "roe_aproximado_pct",
        "utilidad_neta / patrimonio_total * 100",
        ["roe_calculable"],
        "Aproximado; no anualizar automaticamente",
        "porcentaje",
    ),

    (
        EMPRESA,
        "endeudamiento_pct",
        "pasivo_total / activo_total * 100",
        ["endeudamiento_calculable"],
        "No sumar ratios financieros",
        "porcentaje",
    ),

    (
        EMPRESA,
        "margen_neto_pct",
        "utilidad_neta / total_ingreso * 100",
        ["margen_neto_calculable"],
        "No sumar ratios financieros",
        "porcentaje",
    ),

    (
        EMPRESA,
        "autonomia_financiera_pct",
        "patrimonio_total / activo_total * 100",
        ["autonomia_calculable"],
        "No sumar ratios financieros",
        "porcentaje",
    ),
]


COLUMNAS_CONSUMO = {

    MERCADO: [
        "nemonico",
        "fecha",
        "nemonico_original",
        "moneda_referencia",
        "precio_anterior",
        "precio_cierre",
        "precio_maximo",
        "precio_minimo",
        "monto_negociado",
        "retorno_diario_pct",
        "rango_intradia_pct",
        "precio_cierre_valido",
        "precio_anterior_valido",
        "retorno_calculable",
        "rango_intradia_valido",
        "monto_negociado_valido",
        "fecha_ingesta",
    ],

    SAB: [
        "fecha",
        "codigo_sab",
        "sociedad_agente_bolsa",
        "monto_compras",
        "monto_ventas",
        "monto_total",
        "saldo_neto",
        "participacion_total_pct",
        "ranking_diario",
        "hhi_diario",
        "total_actividad_diaria",
        "cantidad_sab_diaria",
        "registros_invalidos_diarios",
        "montos_validos",
        "total_consistente",
        "actividad_diaria_valida",
        "estado_cobertura",
        "cobertura_completa",
        "trazabilidad_verificada",
        "registros_elegibles",
        "registros_cuarentena",
        "archivos_excluidos",
        "scope_quality_id",
        "version_silver",
        "fecha_ingesta",
    ],

    EMPRESA: [
        "id_registro",
        "rpj",
        "ruc",
        "nombre_empresa",
        "fecha_fin_trimestre",
        "moneda",
        "tipo_informacion",
        "activo_total",
        "pasivo_total",
        "patrimonio_total",
        "total_ingreso",
        "utilidad_neta",
        "roa_aproximado_pct",
        "roe_aproximado_pct",
        "endeudamiento_pct",
        "margen_neto_pct",
        "autonomia_financiera_pct",
        "apto_kpi_general",
        "roa_calculable",
        "roe_calculable",
        "endeudamiento_calculable",
        "margen_neto_calculable",
        "autonomia_calculable",
        "colision_financiera",
        "identidad_contable_valida",
        "motivos_calidad",
        "fecha_ingesta",
    ],
}


def tabla(producto):

    exigir(
        producto in COLUMNAS_CONSUMO,
        "Producto no admitido"
    )

    return (
        f"`{PROYECTO}.{DATASET}.{producto}`"
    )


def consulta_detalle(producto, contrato):

    campos = {
        c["name"]
        for c in contrato["gold"]["columns"]
    }

    columnas = COLUMNAS_CONSUMO[producto]

    exigir(
        set(columnas).issubset(campos),
        "Columnas de consumo incompatibles"
    )

    claves = contrato["gold"]["business_keys"]

    exigir(
        set(claves).issubset(columnas),
        "Clave de consumo incompleta"
    )

    fecha = contrato["configuracion"][
        "bigquery_partition"
    ]

    seleccion = ",\n  ".join(
        f"`{c}`" for c in columnas
    )

    orden = ", ".join(
        f"`{c}`" for c in claves
    )

    return CABECERA + f"""
-- Detalle sin agregacion automatica.
-- Conserva filas y controles de calidad.
-- Interpretar indicadores junto con sus flags.

SELECT
  {seleccion}
FROM {tabla(producto)}
WHERE `{fecha}` BETWEEN
  @fecha_inicio AND @fecha_fin
ORDER BY {orden};
"""


def consulta_sab_diaria(contrato):

    campos = {
        c["name"]
        for c in contrato["gold"]["columns"]
    }

    requeridos = {
        "fecha",
        "codigo_sab",
        "total_compras_diario",
        "total_ventas_diario",
        "total_actividad_diaria",
        "hhi_diario",
        "cantidad_sab_diaria",
        "estado_cobertura",
        "cobertura_completa",
        "trazabilidad_verificada",
        "actividad_diaria_valida",
    }

    exigir(
        requeridos.issubset(campos),
        "Contrato SAB incompatible"
    )

    return CABECERA + f"""
-- Una fila por fecha.
-- No sumar los totales repetidos por SAB.
-- Los dias incompletos permanecen visibles.
-- No filtrar intermediarios antes de esta agrupacion.

WITH diario AS (
  SELECT
    fecha,
    COUNT(*) AS filas_sab,
    COUNT(DISTINCT codigo_sab) AS sab_distintos,

    COUNTIF(
      codigo_sab IS NULL
      OR TRIM(codigo_sab) = ''
    ) AS claves_invalidas,

    COUNT(DISTINCT total_compras_diario)
      AS variantes_compras,

    COUNT(DISTINCT total_ventas_diario)
      AS variantes_ventas,

    COUNT(DISTINCT total_actividad_diaria)
      AS variantes_actividad,

    COUNT(DISTINCT hhi_diario)
      AS variantes_hhi,

    COUNT(total_compras_diario)
      AS compras_presentes,

    COUNT(total_ventas_diario)
      AS ventas_presentes,

    COUNT(total_actividad_diaria)
      AS actividad_presente,

    COUNT(hhi_diario)
      AS hhi_presente,

    COUNT(DISTINCT cantidad_sab_diaria)
      AS variantes_cantidad,

    COUNT(cantidad_sab_diaria)
      AS cantidad_presente,

    MAX(cantidad_sab_diaria)
      AS cantidad_sab_gold,

    MAX(total_compras_diario)
      AS compras_candidatas,

    MAX(total_ventas_diario)
      AS ventas_candidatas,

    MAX(total_actividad_diaria)
      AS actividad_candidata,

    MAX(hhi_diario)
      AS hhi_candidato,

    COUNTIF(
      estado_cobertura = 'COMPLETA'
      AND cobertura_completa IS TRUE
      AND trazabilidad_verificada IS TRUE
    ) AS filas_cobertura_completa,

    COUNTIF(
      actividad_diaria_valida IS TRUE
    ) AS filas_actividad_valida,

    COUNTIF(
      estado_cobertura = 'PARCIAL'
    ) AS filas_cobertura_parcial,

    COUNTIF(
      estado_cobertura = 'DESCONOCIDA'
    ) AS filas_cobertura_desconocida

  FROM {tabla(SAB)}
  WHERE fecha BETWEEN
    @fecha_inicio AND @fecha_fin
  GROUP BY fecha
),
evaluado AS (
  SELECT *,
    COALESCE(
      filas_sab = sab_distintos
      AND claves_invalidas = 0
      AND variantes_cantidad = 1
      AND cantidad_presente = filas_sab
      AND cantidad_sab_gold = filas_sab
      AND variantes_compras = 1
      AND variantes_ventas = 1
      AND variantes_actividad = 1
      AND compras_presentes = filas_sab
      AND ventas_presentes = filas_sab
      AND actividad_presente = filas_sab
      AND filas_actividad_valida = filas_sab,
      FALSE
    ) AS totales_consistentes
  FROM diario
)
SELECT
  fecha,
  filas_sab,
  sab_distintos,
  filas_cobertura_completa,
  filas_cobertura_parcial,
  filas_cobertura_desconocida,
  totales_consistentes,

  CASE
    WHEN filas_cobertura_completa = filas_sab
      THEN 'COMPLETA'
    WHEN filas_cobertura_parcial = filas_sab
      THEN 'PARCIAL'
    WHEN filas_cobertura_desconocida = filas_sab
      THEN 'DESCONOCIDA'
    ELSE 'MIXTA_O_INCONSISTENTE'
  END AS cobertura_observada,

  CASE WHEN totales_consistentes
    THEN compras_candidatas
  END AS compras_observadas,

  CASE WHEN totales_consistentes
    THEN ventas_candidatas
  END AS ventas_observadas,

  CASE WHEN totales_consistentes
    THEN actividad_candidata
  END AS actividad_observada,

  CASE
    WHEN totales_consistentes
      AND variantes_hhi = 1
      AND hhi_presente = filas_sab
      AND actividad_candidata > 0
      THEN hhi_candidato
  END AS hhi_observado,

  CASE
    WHEN totales_consistentes
      AND filas_cobertura_completa = filas_sab
      THEN actividad_candidata
  END AS actividad_con_cobertura_completa

FROM evaluado
ORDER BY fecha;
"""


def escribir_json(ruta, contenido):

    with ruta.open("x", encoding="utf-8") as f:

        json.dump(
            contenido,
            f,
            indent=2,
            ensure_ascii=False,
            allow_nan=False,
        )

        f.write("\n")


def generar(raiz):

    raiz = Path(raiz).resolve()

    _, contratos = cargar_contratos(raiz)

    destino = (
        raiz / "docs/bigquery/consumo_matthew"
    )

    sql_dir = destino / "sql_consumo"

    sql_dir.mkdir(
        parents=True,
        exist_ok=False,
    )

    indicadores = []

    for (
        producto,
        nombre,
        formula,
        flags,
        uso,
        unidad,
    ) in INDICADORES:

        campos = {
            c["name"]: c
            for c in contratos[producto]["gold"]["columns"]
        }

        exigir(
            nombre in campos
            and set(flags).issubset(campos),
            "Indicador fuera de contrato"
        )

        exigir(
            all(
                campos[f]["bigquery_type"] == "BOOL"
                for f in flags
            ),
            "Control de indicador incompatible"
        )

        indicadores.append({
            "product": producto,
            "column": nombre,
            "bigquery_type":
                campos[nombre]["bigquery_type"],
            "definition": formula,
            "quality_flags": flags,
            "analytical_restriction": uso,
            "unit": unidad,
            "automatic_aggregation": None,
            "existing_gold_indicator": True,
            "operatively_approved": False,
        })

    exigir(
        len(indicadores) == 13,
        "Catalogo incompleto"
    )

    ruta_catalogo = (
        destino / "indicadores_gold_documentados.json"
    )

    escribir_json(
        ruta_catalogo,
        {
            "status": ESTADO,
            "indicators": indicadores,
            "indicator_count": len(indicadores),
            "production_consumption_authorized":
                False,
        },
    )

    consultas = {}

    for producto, contrato in contratos.items():

        consultas[
            f"{producto}_detalle.sql"
        ] = consulta_detalle(
            producto,
            contrato,
        )

    consultas[
        "actividad_sab_resumen_diario.sql"
    ] = consulta_sab_diaria(
        contratos[SAB]
    )

    rutas = [ruta_catalogo]

    for nombre, contenido in sorted(
        consultas.items()
    ):

        ruta = sql_dir / nombre

        with ruta.open(
            "x",
            encoding="utf-8",
        ) as f:

            f.write(contenido)

        rutas.append(ruta)

    referencias = []

    for ruta in rutas:

        contenido = ruta.read_bytes()

        referencias.append({
            "path": ruta.relative_to(
                raiz
            ).as_posix(),
            "sha256": hashlib.sha256(
                contenido
            ).hexdigest(),
            "size_bytes": len(contenido),
        })

    escribir_json(
        destino / "inventario_guia_consumo.json",
        {
            "status": ESTADO,
            "files": referencias,
            "query_count": len(consultas),
            "indicator_count": len(indicadores),
            "google_sql_engine_validated": False,
            "cloud_execution_authorized": False,
            "production_consumption_authorized":
                False,
            "publication_authorized": False,
        },
    )

    print("INDICADORES_DOCUMENTADOS: 13/13")
    print("CONSULTAS_CONSUMO_GENERADAS: 4/4")


if __name__ == "__main__":

    generar(Path.cwd())
