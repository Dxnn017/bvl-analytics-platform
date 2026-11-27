"""Registro de interfaces de ejecución incremental Gold.

Describe los ejecutores locales disponibles.
No ejecuta Spark ni publica datos.
"""

from pathlib import Path


DIRECTORIO = Path(__file__).resolve().parent

EJECUTORES_LOCALES = {
    "mercado_valor_diario": {
        "archivo": "ejecutar_mercado_valor_local.py",
        "integracion": "REQUIERE_CLAVES_AFECTADAS",
        "parametros": [
            "--silver-root",
            "--gold-base-path",
            "--claves-afectadas",
        ],
    },
    "actividad_sab_diaria": {
        "archivo": "ejecutar_actividad_sab_local.py",
        "integracion": "REQUIERE_TAREA_RESUELTA",
        "parametros": ['--tarea-planificada', '--silver-path', '--gold-base-path'],
    },
    "empresa_trimestre": {
        "archivo": "ejecutar_empresa_trimestre_local.py",
        "integracion": "REQUIERE_TAREA_RESUELTA",
        "parametros": ['--tarea-planificada', '--silver-path', '--gold-base-path'],
    },
    "hechos_empresa": {
        "archivo": "ejecutar_hechos_empresa_local.py",
        "integracion": "REQUIERE_EXPEDIENTES",
        "parametros": ['--expediente', '--silver-root', '--gold-base-path'],
    },
    "impacto_hechos_valor": {
        "archivo": "ejecutar_impacto_hechos_valor_local.py",
        "integracion": "REQUIERE_ALCANCE_GOLD_GOLD",
        "parametros": ['--expediente', '--cotizacion-afectada', '--hechos-path', '--mercado-path', '--impacto-base-path'],
    },
}


def obtener_ejecutor_local(producto):
    """Devuelve la configuración de un ejecutor conocido."""

    if producto not in EJECUTORES_LOCALES:
        raise ValueError(
            f"Producto Gold no reconocido: {producto}"
        )

    configuracion = dict(EJECUTORES_LOCALES[producto])
    configuracion["ruta"] = str(
        DIRECTORIO / configuracion["archivo"]
    )

    return configuracion


def listar_ejecutores_locales():
    """Devuelve las interfaces disponibles sin ejecutarlas."""

    return {
        producto: obtener_ejecutor_local(producto)
        for producto in EJECUTORES_LOCALES
    }
