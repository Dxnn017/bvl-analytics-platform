"""Recálculo incremental de actividad SAB por fechas completas.

No escribe datos ni publica versiones Gold.
"""

from pyspark.sql import functions as F

from build_actividad_sab_diaria import construir_gold, validar_clave


def recalcular_actividad_sab(
    silver_sab, gold_base, fechas_afectadas,
    cobertura=None, version_silver=None,
):
    """Reconstruye las fechas afectadas y conserva el resto de Gold."""

    fechas = fechas_afectadas.select(
        F.to_date("fecha").alias("fecha")
    ).distinct()

    if cobertura:
        from datetime import date

        fechas_cobertura = silver_sab.sparkSession.createDataFrame(
            [(date.fromisoformat(r["fecha_negociacion"]),)
             for r in cobertura],
            "fecha date",
        )

        fechas = fechas.unionByName(fechas_cobertura).distinct()

    if fechas.filter(F.col("fecha").isNull()).limit(1).count():
        raise ValueError("Hay fechas afectadas inválidas.")

    # Recuperar todas las SAB de cada fecha afectada.
    silver_afectado = silver_sab.join(
        fechas,
        silver_sab["fecha_negociacion"] == fechas["fecha"],
        "left_semi",
    )

    validar_clave(
        silver_afectado,
        ["fecha_negociacion", "codigo_sab"],
        "Silver SAB afectado",
    )

    gold_recalculado = construir_gold(
        silver_afectado, cobertura, version_silver
    )

    from cobertura_sab import propagar_cobertura, COLUMNAS_COBERTURA

    if not set(COLUMNAS_COBERTURA).issubset(gold_base.columns):
        gold_base = propagar_cobertura(gold_base)

    # Conservar las fechas no afectadas, incluyendo otros meses.
    gold_conservado = gold_base.join(
        fechas,
        "fecha",
        "left_anti",
    )

    # No trasladar evidencia de cobertura entre versiones Silver.
    # Sin version acreditada, la cobertura conservada es desconocida.
    version_vigente = (
        F.lit(version_silver).cast("string")
        if version_silver
        else F.lit(None).cast("string")
    )

    conservar_evidencia = (
        F.col("version_silver").isNotNull()
        & version_vigente.isNotNull()
        & F.col("version_silver").eqNullSafe(version_vigente)
    )

    gold_conservado = gold_conservado.withColumn(
        "_conservar_evidencia", conservar_evidencia
    )

    conservar_evidencia = F.col("_conservar_evidencia")

    for campo in (
        "registros_elegibles",
        "registros_cuarentena",
        "archivos_excluidos",
        "version_silver",
        "scope_quality_id",
    ):
        tipo = gold_conservado.schema[campo].dataType

        gold_conservado = gold_conservado.withColumn(
            campo,
            F.when(conservar_evidencia, F.col(campo))
             .otherwise(F.lit(None).cast(tipo))
        )

    gold_conservado = (
        gold_conservado
        .withColumn(
            "estado_cobertura",
            F.when(conservar_evidencia, F.col("estado_cobertura"))
             .otherwise(F.lit("DESCONOCIDA"))
        )
        .withColumn(
            "trazabilidad_verificada",
            F.when(conservar_evidencia, F.col("trazabilidad_verificada"))
             .otherwise(F.lit(False))
        )
        .withColumn(
            "cobertura_completa",
            F.when(conservar_evidencia, F.col("cobertura_completa"))
             .otherwise(F.lit(False))
        )
    )

    gold_conservado = gold_conservado.drop("_conservar_evidencia")

    # Una fecha eliminada completamente en Silver desaparece de Gold.
    gold_candidato = gold_conservado.unionByName(
        gold_recalculado
    )

    validar_clave(
        gold_candidato,
        ["fecha", "codigo_sab"],
        "Gold SAB candidato",
    )

    return gold_candidato


def resolver_fechas_sab(spark, dataset):
    """Convierte cambios efectivos Silver en fechas completas para SAB."""

    from scope_resolver import (
        obtener_fechas_afectadas,
        construir_dataframe_fechas,
    )

    if not isinstance(dataset, dict):
        raise ValueError("El detalle Silver debe ser un diccionario.")

    if dataset.get("dataset") not in (None, "montos_sab"):
        raise ValueError("El detalle no corresponde a montos_sab.")

    fechas = obtener_fechas_afectadas(dataset)

    if dataset["total_cambios_efectivos"] > 0 and not fechas:
        raise ValueError(
            "Existen cambios Silver sin fechas anteriores ni nuevas."
        )

    return construir_dataframe_fechas(spark, fechas)


def resolver_fechas_sab_desde_tarea(spark, tarea):
    """Obtiene fechas SAB desde una tarea del planificador Gold."""

    from datetime import date
    from scope_resolver import construir_dataframe_fechas

    if tarea.get("producto_gold") != "actividad_sab_diaria":
        raise ValueError("La tarea no corresponde a actividad_sab_diaria.")

    if tarea.get("detalle_externo") or tarea.get("requiere_resolver_detalle_externo"):
        raise ValueError("Existen detalles externos sin resolver.")

    cambios = tarea.get("cambios_directos_declarados")
    if not isinstance(cambios, list) or not cambios:
        raise ValueError("La tarea no contiene cambios directos Silver.")

    fechas = set()

    for cambio in cambios:
        if cambio.get("dataset") != "montos_sab":
            raise ValueError("Dataset Silver incompatible con SAB.")

        operaciones = cambio.get("operaciones_declaradas")
        if not isinstance(operaciones, list):
            raise ValueError("Falta el detalle de operaciones Silver.")

        if len(operaciones) != cambio.get("total_cambios_declarados"):
            raise ValueError("Cantidad de operaciones incompatible.")

        for operacion in operaciones:
            tipo = operacion.get("operacion")
            if tipo not in {"INSERT", "UPDATE", "DELETE"}:
                raise ValueError("Operacion Silver no soportada.")

            for campo in ("clave_anterior", "clave_nueva"):
                clave = operacion.get(campo)
                if clave is None:
                    continue

                if not isinstance(clave, dict):
                    raise ValueError("Clave Silver invalida.")

                valor = clave.get("fecha_negociacion")
                if not isinstance(valor, str):
                    raise ValueError("Falta fecha_negociacion.")

                fechas.add(date.fromisoformat(valor))

    if not fechas:
        raise ValueError("No se encontraron fechas SAB afectadas.")

    return construir_dataframe_fechas(spark, sorted(fechas))
