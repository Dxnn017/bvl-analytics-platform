"""Adaptacion numerica candidata Silver-Gold.

No modifica Silver oficial ni constructores.
No reemplaza valores invalidos por NULL o cero.
No concede autorizaciones Quality.
"""

from pyspark.sql import functions as F


REGLAS = {
    "mercado_diario/cotizaciones": {
        "apertura_actual": "double",
        "cierre_actual": "double",
        "cierre_anterior": "double",
        "maxima_actual": "double",
        "minima_actual": "double",
        "monto_negociado": "double",
        "promedio_actual": "double",
    },
    "mercado_diario/montos_sab": {
        "MontoCompras": "decimal(24,2)",
        "MontoVentas": "decimal(24,2)",
        "MontoTotal": "decimal(24,2)",
        "PorcentajeCompras": "double",
        "PorcentajeVentas": "double",
        "PorcentajeTotal": "double",
    },
}

POLITICA = "STRICT_NUMERIC_CONVERSION_V1"

PATRON_DOUBLE = (
    r"^[+-]?(?:[0-9]+(?:\.[0-9]*)?"
    r"|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$"
)

# Admite decimales con ceros adicionales.
# No admite fracciones que requieran redondeo.
PATRON_DECIMAL = (
    r"^[+-]?(?:[0-9]+(?:\.[0-9]{0,2}0*)?"
    r"|\.[0-9]{1,2}0*)$"
)


def exigir(condicion, mensaje):

    if not condicion:
        raise ValueError(mensaje)


def tipos_dataframe(df):

    return {
        campo.name: campo.dataType.simpleString()
        for campo in df.schema.fields
    }


def expresiones_conversion(nombre, tipo):

    original = F.col(nombre)
    limpio = F.trim(original)

    convertido = F.expr(
        f"try_cast(trim(`{nombre}`) as {tipo})"
    )

    if tipo == "double":

        formato = limpio.rlike(PATRON_DOUBLE)

        mantisa = F.regexp_extract(
            limpio,
            r"^([^eE]+)",
            1
        )

        subdesbordamiento = (
            (convertido == 0.0)
            & mantisa.rlike(r"[1-9]")
        )

        invalido = (
            ~formato
            | convertido.isNull()
            | F.isnan(convertido)
            | (F.abs(convertido) == float("inf"))
            | subdesbordamiento
        )

    elif tipo == "decimal(24,2)":

        formato = limpio.rlike(PATRON_DECIMAL)

        invalido = (
            ~formato
            | convertido.isNull()
        )

    else:

        raise ValueError(
            "Tipo de conversion no permitido"
        )

    invalido = (
        original.isNotNull()
        & F.coalesce(invalido, F.lit(True))
    )

    return convertido, invalido


def adaptar_numericos(df, fuente):

    exigir(
        fuente in REGLAS,
        "Fuente sin regla numerica aprobable"
    )

    exigir(
        len(df.columns) == len(set(df.columns)),
        "Columnas de entrada duplicadas"
    )

    reglas = REGLAS[fuente]

    tipos_entrada = tipos_dataframe(df)

    conversiones = {}
    controles = []

    for nombre, tipo in reglas.items():

        exigir(
            tipos_entrada.get(nombre) == "string",
            f"Tipo de origen incompatible: {nombre}"
        )

        convertido, invalido = expresiones_conversion(
            nombre, tipo
        )

        conversiones[nombre] = convertido

        controles.extend([
            F.count(
                F.when(invalido, 1)
            ).alias(nombre + "__invalidos"),

            F.count(
                F.when(
                    F.col(nombre).isNull(), 1
                )
            ).alias(nombre + "__nulos"),

            F.count(
                F.when(
                    F.col(nombre).isNotNull(), 1
                )
            ).alias(nombre + "__presentes"),
        ])

    metricas = df.agg(
        F.count("*").alias("__registros"),
        *controles
    ).first().asDict()

    errores = {
        nombre: metricas[nombre + "__invalidos"]
        for nombre in reglas
        if metricas[nombre + "__invalidos"] > 0
    }

    exigir(
        not errores,
        f"CONVERSION_NUMERICA_RECHAZADA: "
        f"{fuente}: {errores}"
    )

    resultado = df.select(*[
        conversiones[nombre].alias(nombre)
        if nombre in conversiones
        else F.col(nombre)
        for nombre in df.columns
    ])

    tipos_esperados = {
        **tipos_entrada,
        **reglas,
    }

    exigir(
        tipos_dataframe(resultado)
        == tipos_esperados,
        "Esquema adaptado incompatible"
    )

    exigir(
        resultado.columns == df.columns,
        "La adaptacion modifico las columnas"
    )

    evidencia = {
        "status": "VALIDATED_LOCAL_TRANSFORMATION",
        "product": fuente,
        "policy": POLITICA,
        "input_records": metricas["__registros"],
        "columns": {
            nombre: {
                "input_type": "string",
                "output_type": tipo,
                "converted_records":
                    metricas[nombre + "__presentes"],
                "original_nulls":
                    metricas[nombre + "__nulos"],
                "invalid_records":
                    metricas[nombre + "__invalidos"],
            }
            for nombre, tipo in reglas.items()
        },
        "rounding_monetary_allowed": False,
        "invalid_replacement_allowed": False,
        "silver_modified": False,
        "constructor_modified": False,
        "publication_allowed": False,
    }

    return resultado, evidencia
