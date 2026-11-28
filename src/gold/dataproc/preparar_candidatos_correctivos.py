"""Preparacion correctiva Gold, sin escrituras.

Reutiliza constructores ratificados.
La autorizacion correctiva debe validarse antes de operar.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from autorizacion_correctiva_quality import (
    exigir,
    sha256,
    json_estricto,
    verificar_firma_oficial,
    verificar_vinculo_fuente,
    validar_autorizacion_correctiva,
)

from flujo_silver_gold_v03 import (
    preparar_silver_v03_con_parquet,
    leer_verificado,
)

from integracion_quality_v03 import (
    materializar_fuente_verificada,
    verificar_vigencia,
    MAX_BYTES_CANDIDATO,
)

from adaptador_cotizaciones_quality import (
    adaptar_cotizaciones,
)

from adaptador_numerico_integral import (
    adaptar_numericos,
    REGLAS,
)

from autorizacion_correctiva_quality import (
    validar_decision_numerica,
)

from sidecar_sab_ratificado import (
    interpretar_sidecar_ratificado,
)

from ejecutor_portable import (
    construir_candidato,
    validar_claves,
    CLAVES,
)


ORDEN = (
    "mercado_valor_diario",
    "actividad_sab_diaria",
    "empresa_trimestre",
)

DEPENDENCIAS = {
    "mercado_valor_diario": {
        "cotizaciones": "mercado_diario/cotizaciones",
        "tipo_cambio": "macroeconomia/tipo_cambio",
        "embig": "macroeconomia/embig",
        "puente_ruc_nemonico":
            "referencias_smv/puente_ruc_nemonico",
    },
    "actividad_sab_diaria": {
        "montos_sab": "mercado_diario/montos_sab",
    },
    "empresa_trimestre": {
        "principales_cuentas":
            "finanzas_empresariales/principales_cuentas",
    },
}

COLUMNAS_ESPERADAS = {
    "mercado_valor_diario": 41,
    "actividad_sab_diaria": 41,
    "empresa_trimestre": 58,
}

PARTICIONES = {
    "mercado_valor_diario": ["anio", "mes"],
    "actividad_sab_diaria": ["anio", "mes"],
    "empresa_trimestre": ["anio", "trimestre"],
}

CONTEOS = {
    "mercado_valor_diario": 83130,
    "actividad_sab_diaria": 21911,
    "empresa_trimestre": 5158,
}


def verificar_fuente(
    cliente, auth, producto, fuente,
    evidencia, pem,
):

    contenido = evidencia["autorizacion_bytes"]
    firma = evidencia["firma_autorizacion_der"]
    contrato_bytes = evidencia["contrato_fisico_bytes"]

    autorizacion = verificar_vinculo_fuente(
        auth,
        fuente,
        contenido,
        firma,
        contrato_bytes,
    )

    contrato = json_estricto(contrato_bytes)

    exigir(
        set(contrato) == {
            "product",
            "version_id",
            "tipos_esperados",
            "claves",
            "particiones_por_uri",
            "schema_spark",
        },
        "Contrato fisico incompleto"
    )

    exigir(
        contrato["product"] == fuente
        and contrato["version_id"]
        == autorizacion["version_id"],
        "Contrato fisico de otra fuente"
    )

    esquema = StructType.fromJson(
        contrato["schema_spark"]
    )

    evidencia_fisica = {
        "autorizacion_bytes": contenido,
        "firma_autorizacion_der": firma,
        "tipos_esperados": contrato["tipos_esperados"],
        "claves": contrato["claves"],
        "particiones_por_uri":
            contrato["particiones_por_uri"],
        "schema_spark": esquema,
    }

    resultado = preparar_silver_v03_con_parquet(
        cliente,
        contenido,
        firma,
        pem,
        producto,
        evidencia_fisica["tipos_esperados"],
        evidencia_fisica["claves"],
        evidencia_fisica["particiones_por_uri"],
    )

    exigir(
        resultado["product"] == fuente
        and resultado["version_id"]
        == autorizacion["version_id"]
        and resultado["esquema_fisico_verificado"] is True
        and resultado["consumo_productivo_autorizado"] is False,
        "Preflight Silver incompatible"
    )

    manifest = json_estricto(
        leer_verificado(
            cliente, autorizacion["manifest"]
        )
    )

    exigir(
        manifest["product"] == fuente
        and manifest["version_id"]
        == autorizacion["version_id"]
        and manifest["records_count"]
        == resultado["filas_validadas"]
        and len(manifest["files"])
        == resultado["archivos_parquet_verificados"],
        "Manifest Silver incompatible"
    )

    return {
        "auth": autorizacion,
        "manifest": manifest,
        "preflight": resultado,
        "evidencia": evidencia_fisica,
    }


def cargar_cobertura_acreditada(
    cliente, auth, pem, fuente_verificada,
):

    referencia = auth["sab_coverage"]

    if referencia is None:
        return None, None

    auth_silver = fuente_verificada["auth"]

    manifest = fuente_verificada["manifest"]

    preflight = fuente_verificada["preflight"]

    exigir(
        auth_silver["product"]
        == manifest["product"]
        == preflight["product"]
        == "mercado_diario/montos_sab",
        "Fuente de cobertura incompatible"
    )

    exigir(
        auth_silver["version_id"]
        == manifest["version_id"]
        == preflight["version_id"]
        == referencia["version_id"],
        "Version de cobertura incompatible"
    )

    exigir(
        preflight["esquema_fisico_verificado"] is True
        and preflight["filas_validadas"]
        == manifest["records_count"],
        "Preflight SAB incompatible"
    )

    filas, evidencia = interpretar_sidecar_ratificado(
        contenido=leer_verificado(
            cliente, referencia["payload"]
        ),
        firma=leer_verificado(
            cliente, referencia["signature"]
        ),
        ratificacion_bytes=leer_verificado(
            cliente, referencia["ratification"]
        ),
        ratificacion_firma=leer_verificado(
            cliente, referencia["ratification_signature"]
        ),
        pem=pem,
        version_id=referencia["version_id"],
        manifest_sha256=auth_silver["manifest"]["sha256"],
        manifest_signature_sha256=(
            auth_silver["manifest_signature"]["sha256"]
        ),
        manifest_generation=(
            auth_silver["manifest"]["generation"]
        ),
        registros_esperados=manifest["records_count"],
    )

    exigir(
        evidencia["policy"] == referencia["policy"],
        "Politica de cobertura incompatible"
    )

    return filas, evidencia


def adaptar_fuente_numerica(
    df, fuente, auth, verificada,
):

    decision = validar_decision_numerica(auth)

    referencia = decision["sources"][fuente]

    auth_silver = verificada["auth"]
    manifest = verificada["manifest"]
    preflight = verificada["preflight"]

    exigir(
        auth_silver["product"]
        == manifest["product"]
        == preflight["product"]
        == fuente,
        "Identidad de adaptacion incompatible"
    )

    exigir(
        auth_silver["version_id"]
        == manifest["version_id"]
        == preflight["version_id"]
        == referencia["version_id"],
        "Version de adaptacion incompatible"
    )

    exigir(
        preflight["esquema_fisico_verificado"] is True
        and preflight["filas_validadas"]
        == manifest["records_count"],
        "Preflight numerico incompatible"
    )

    resultado, evidencia = adaptar_numericos(
        df, fuente
    )

    exigir(
        evidencia["input_records"]
        == manifest["records_count"],
        "Conteo de adaptacion incompatible"
    )

    evidencia.update({
        "status": "APPLIED",
        "version_id": referencia["version_id"],
        "physical_contract_sha256":
            referencia["physical_contract_sha256"],
        "authorization_id": auth["authorization_id"],
        "decision_id": decision["decision_id"],
        "manifest_reference": auth_silver["manifest"],
    })

    return resultado, evidencia


def validar_claves_conservadas(
    producto, candidato, fuente,
):

    if producto == "mercado_valor_diario":

        origen = fuente.select(
            F.upper(F.trim("valor")).alias("nemonico"),
            F.col("fecha_cotizacion").alias("fecha"),
        )

    elif producto == "actividad_sab_diaria":

        origen = fuente.select(
            F.col("fecha_negociacion").alias("fecha"),
            "codigo_sab",
        )

    else:

        return

    destino = candidato.select(*origen.columns)

    exigir(
        not origen.exceptAll(destino).limit(1).count()
        and not destino.exceptAll(origen).limit(1).count(),
        "Constructor altero claves de negocio"
    )


def validar_contrato_salida(
    producto, df, contrato_bytes, auth,
):

    exigir(
        sha256(contrato_bytes)
        == auth["output_contracts"][producto],
        "Contrato Gold no acreditado"
    )

    contrato = json_estricto(contrato_bytes)

    exigir(
        contrato["product"] == producto
        and contrato["production_authorized"] is False
        and contrato["business_keys"] == list(CLAVES[producto])
        and contrato["source_partition_columns"]
        == PARTICIONES[producto],
        "Contrato Gold incompatible"
    )

    campos = contrato["columns"]

    nombres = [campo["name"] for campo in campos]

    exigir(
        len(nombres) == COLUMNAS_ESPERADAS[producto]
        and len(nombres) == len(set(nombres))
        and len(df.columns) == len(nombres)
        and set(df.columns) == set(nombres),
        "Columnas Gold incompatibles"
    )

    # No realizar cast silencioso ni agregar columnas.
    tipos = {
        campo.name: campo.dataType.simpleString()
        for campo in df.schema.fields
    }

    condiciones = F.lit(False)

    for campo in campos:

        nombre = campo["name"]
        tipo = campo["spark_type"]

        exigir(
            tipos[nombre] == tipo,
            f"Tipo incompatible: {nombre}: "
            f"{tipos[nombre]} != {tipo}"
        )

        if campo["nullable"] is False:

            condiciones = (
                condiciones | F.col(nombre).isNull()
            )

    exigir(
        not df.filter(condiciones).limit(1).count(),
        "Nulos en campos obligatorios"
    )

    validar_claves(df, producto)

    for particion in PARTICIONES[producto]:

        exigir(
            not df.filter(
                F.col(particion).isNull()
            ).limit(1).count(),
            "Particion Gold nula"
        )

    return df.select(*nombres)


def digest_ids(df):
    """SHA-256 de IDs ordenados, UTF-8, LF final."""

    import re

    identificadores = [
        fila["id_registro"]
        for fila in df.select("id_registro").collect()
    ]

    exigir(
        all(
            isinstance(v, str)
            and re.fullmatch(r"[0-9a-f]{64}", v)
            for v in identificadores
        ),
        "Identificador financiero invalido"
    )

    exigir(
        len(identificadores)
        == len(set(identificadores)),
        "Identificador financiero duplicado"
    )

    contenido = "".join(
        identificador + "\n"
        for identificador in sorted(identificadores)
    ).encode("utf-8")

    return len(identificadores), sha256(contenido)


def separar_finanzas(df, fuente, decision):

    validar_claves(df, "empresa_trimestre")
    validar_claves(fuente, "empresa_trimestre")

    exigir(
        df.count() == decision["expected_source_rows"]
        and fuente.count() == decision["expected_source_rows"],
        "Conteo financiero de origen incompatible"
    )

    ids_fuente = fuente.select("id_registro")
    ids_gold = df.select("id_registro")

    exigir(
        not ids_fuente.exceptAll(ids_gold).limit(1).count()
        and not ids_gold.exceptAll(ids_fuente).limit(1).count(),
        "Constructor financiero altero identificadores"
    )

    ruc_valido = F.coalesce(
        F.col("ruc_valido"),
        F.lit(False)
    )

    colision = F.coalesce(
        F.col("colision_financiera"),
        F.lit(True)
    )

    ruc = df.filter(~ruc_valido)

    bnb = df.filter(ruc_valido & colision)

    aceptados = df.filter(ruc_valido & ~colision)

    exigir(
        not bnb.filter(
            ~F.col("ruc").eqNullSafe(
                F.lit("20513808519")
            )
            | ~F.col("rpj").eqNullSafe(
                F.lit("SG0005")
            )
        ).limit(1).count(),
        "Colision financiera no conciliada"
    )

    grupos = (
        (
            ruc,
            85,
            "excluded_ruc_ids_sha256"
        ),
        (
            bnb,
            6,
            "excluded_bnb_ids_sha256"
        ),
        (
            aceptados,
            5158,
            "accepted_ids_sha256"
        ),
    )

    for conjunto, esperado, campo in grupos:

        cantidad, huella = digest_ids(conjunto)

        exigir(
            cantidad == esperado,
            f"Conteo financiero incompatible: {campo}"
        )

        exigir(
            huella == decision[campo],
            f"Conjunto nominal incompatible: {campo}"
        )

    kpis = [
        "roa_aproximado_pct",
        "roe_aproximado_pct",
        "endeudamiento_pct",
        "margen_neto_pct",
        "autonomia_financiera_pct",
    ]

    tiene_kpi = F.lit(False)

    for campo in kpis:

        tiene_kpi |= F.col(campo).isNotNull()

        exigir(
            not aceptados.filter(
                F.isnan(F.col(campo))
                | (F.abs(F.col(campo)) == float("inf"))
            ).limit(1).count(),
            f"KPI financiero no finito: {campo}"
        )

    exigir(
        not aceptados.filter(
            ~F.coalesce(
                F.col("apto_kpi_general"),
                F.lit(False)
            ) & tiene_kpi
        ).limit(1).count(),
        "KPI presente en registro no elegible"
    )

    # Las excepciones mantienen sus registros completos.
    # No son candidatos habilitados para consumo de KPIs.

    return aceptados, {
        "finanzas_ruc_no_conciliado": ruc,
        "finanzas_ruc_valido_ambiguo": bnb,
    }


def validar_contenido(producto, df, fuente, cobertura):

    exigir(
        df.count() == CONTEOS[producto],
        "Conteo correctivo inesperado: " + producto
    )

    if producto == "mercado_valor_diario":

        exigir(
            not df.filter(
                F.col("nemonico_original").isNull()
                | ~F.col("nemonico").eqNullSafe(
                    F.upper(F.trim("nemonico_original"))
                )
            ).limit(1).count(),
            "Normalizacion Mercado incompatible"
        )

    elif producto == "actividad_sab_diaria":

        if cobertura is None:

            exigir(
                not df.filter(
                    ~F.col("estado_cobertura").eqNullSafe(
                        F.lit("DESCONOCIDA")
                    )
                    | F.col("trazabilidad_verificada")
                    | F.col("cobertura_completa")
                    | F.col("scope_quality_id").isNotNull()
                    | F.col("version_silver").isNotNull()
                    | F.col("registros_elegibles").isNotNull()
                    | F.col("registros_cuarentena").isNotNull()
                    | F.col("archivos_excluidos").isNotNull()
                ).limit(1).count(),
                "Cobertura SAB sin acreditacion"
            )

        else:

            fechas = {
                fila["fecha"].isoformat():
                    fila["cantidad_sab_diaria"]
                for fila in df.select(
                    "fecha", "cantidad_sab_diaria"
                ).distinct().collect()
            }

            for registro in cobertura:

                fecha = registro["fecha_negociacion"]
                elegibles = registro["registros_elegibles"]

                if elegibles is not None:

                    exigir(
                        fechas.get(fecha, 0) == elegibles,
                        "Cobertura sin correspondencia diaria"
                    )

    if producto != "empresa_trimestre":

        exigir(
            df.count() == fuente.count(),
            "Constructor altero conteo fuente principal"
        )


def preparar_candidatos(
    spark, cliente,
    autorizacion_bytes,
    firma_autorizacion,
    pem,
    identidad,
    evidencias,
    contratos_salida,
):

    auth = validar_autorizacion_correctiva(
        autorizacion_bytes,
        firma_autorizacion,
        pem,
        identidad,
    )

    exigir(
        set(evidencias) == set(auth["sources"]),
        "Evidencias Silver incompletas"
    )

    exigir(
        set(contratos_salida) == set(ORDEN),
        "Contratos Gold incompletos"
    )

    verificadas = {}
    dataframes = {}

    for producto in ORDEN:

        verificadas[producto] = {}

        for alias, fuente in DEPENDENCIAS[producto].items():

            verificadas[producto][alias] = verificar_fuente(
                cliente,
                auth,
                producto,
                fuente,
                evidencias[fuente],
                pem,
            )

    total_bytes = sum(
        fuente["manifest"]["bytes_total"]
        for grupo in verificadas.values()
        for fuente in grupo.values()
    )

    exigir(
        total_bytes <= MAX_BYTES_CANDIDATO,
        "Limite candidato de materializacion excedido"
    )

    cobertura, evidencia_cobertura = (
        cargar_cobertura_acreditada(
            cliente, auth, pem,
            verificadas["actividad_sab_diaria"]["montos_sab"],
        )
    )

    def comprobar_vigencia():

        validar_autorizacion_correctiva(
            autorizacion_bytes,
            firma_autorizacion,
            pem,
            identidad,
        )

        for producto, fuentes in verificadas.items():

            verificar_vigencia(
                cliente, producto, fuentes, pem
            )

    comprobar_vigencia()

    candidatos = {}
    excepciones = {}
    evidencia_adaptacion = None
    evidencias_numericas = {}

    for producto in ORDEN:

        dataframes[producto] = {
            alias: materializar_fuente_verificada(
                spark, cliente, fuente
            )
            for alias, fuente
            in verificadas[producto].items()
        }

        if producto == "mercado_valor_diario":

            comprobar_vigencia()

            (
                dataframes[producto]["cotizaciones"],
                evidencia_adaptacion,
            ) = adaptar_cotizaciones(
                dataframes[producto]["cotizaciones"],
                auth,
                verificadas[producto]["cotizaciones"],
            )

        for alias, fuente in DEPENDENCIAS[producto].items():

            if fuente not in REGLAS:
                continue

            comprobar_vigencia()

            (
                dataframes[producto][alias],
                evidencias_numericas[fuente],
            ) = adaptar_fuente_numerica(
                dataframes[producto][alias],
                fuente,
                auth,
                verificadas[producto][alias],
            )

        parametros = {}

        if producto == "actividad_sab_diaria":

            parametros = {
                "cobertura": cobertura,
                "version_silver": auth["sources"][
                    "mercado_diario/montos_sab"
                ]["version_id"],
            }

        comprobar_vigencia()

        candidato = construir_candidato(
            producto,
            dataframes[producto],
            **parametros
        )

        candidato = validar_contrato_salida(
            producto,
            candidato,
            contratos_salida[producto],
            auth,
        )

        if producto == "empresa_trimestre":

            candidato, excepciones = separar_finanzas(
                candidato,
                dataframes[producto]["principales_cuentas"],
                auth["financial_decision"],
            )

        fuente_principal = next(
            iter(dataframes[producto].values())
        )

        validar_contenido(
            producto,
            candidato,
            fuente_principal,
            cobertura,
        )

        validar_claves_conservadas(
            producto,
            candidato,
            fuente_principal,
        )

        if producto == "actividad_sab_diaria":

            if evidencia_cobertura is not None:

                observados = {
                    fila["fecha"].isoformat():
                        fila["cantidad_sab_diaria"]
                    for fila in candidato.select(
                        "fecha", "cantidad_sab_diaria"
                    ).distinct().collect()
                }

                esperados = {
                    fila["fecha_negociacion"]:
                        fila["registros_elegibles"]
                    for fila in cobertura
                }

                exigir(
                    observados == esperados,
                    "Cobertura diaria incompleta o incompatible"
                )

                exigir(
                    not candidato.filter(
                        ~F.col("estado_cobertura").eqNullSafe(
                            F.lit("DESCONOCIDA")
                        )
                        | F.col("cobertura_completa")
                        | F.col("trazabilidad_verificada")
                        | F.col("scope_quality_id").isNotNull()
                        | F.col("registros_cuarentena").isNotNull()
                        | F.col("archivos_excluidos").isNotNull()
                        | ~F.col("version_silver").eqNullSafe(
                            F.lit(evidencia_cobertura["version_id"])
                        )
                        | ~F.col("registros_elegibles").eqNullSafe(
                            F.col("cantidad_sab_diaria")
                        )
                    ).limit(1).count(),
                    "Propagacion SAB incompatible"
                )

                evidencia_cobertura[
                    "daily_counts_verified_against_dataframe"
                ] = True

        candidatos[producto] = candidato

    exigir(
        set(evidencias_numericas) == set(REGLAS),
        "Adaptaciones numericas incompletas"
    )

    comprobar_vigencia()

    return {
        "autorizacion": auth,
        "candidatos": candidatos,
        "excepciones": excepciones,
        "cobertura_acreditada": cobertura is not None,
        "cotizaciones_adaptation": evidencia_adaptacion,
        "numeric_adaptations": evidencias_numericas,
        "sab_coverage_evidence": evidencia_cobertura,

        "verificar_vigencia": comprobar_vigencia,
        "publication_allowed": False,
    }
