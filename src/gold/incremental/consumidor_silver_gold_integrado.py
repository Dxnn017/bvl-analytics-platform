"""Consumidor Silver–Gold integrado para pruebas locales.

No autoriza consumo productivo ni publica versiones.
"""

from flujo_silver_gold_seguro import preparar_consumo_silver
from vincular_archivos_manifest import validar_archivos_del_manifest
from validar_esquema_particiones import validar_esquema_particiones


def preparar_consumo_integrado(
    cliente,
    referencia_current,
    firma_current,
    referencia_manifest,
    firma_manifest,
    clave_publica_confiable,
    autorizacion,
    columnas_requeridas,
    particiones_requeridas,
):
    resultado = preparar_consumo_silver(
        cliente,
        referencia_current,
        firma_current,
        referencia_manifest,
        firma_manifest,
        clave_publica_confiable,
        autorizacion,
    )

    esquema = validar_esquema_particiones(
        resultado['manifest'],
        columnas_requeridas,
        particiones_requeridas,
    )

    archivos = validar_archivos_del_manifest(
        cliente,
        resultado["manifest"],
        resultado["version_id"],
    )

    return {
        "producto": resultado["producto"],
        "version_id": resultado["version_id"],
        "archivos_validados": archivos["archivos_validados"],
        "integridad_local_correcta": True,
        "metadatos_esquema_validos": esquema["metadatos_esquema_validos"],
        "esquema_parquet_verificado": False,
        "consumo_productivo_autorizado": False,
    }
