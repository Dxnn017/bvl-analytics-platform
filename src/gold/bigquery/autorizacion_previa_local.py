"""Control previo local. No acredita permisos oficiales de GCP."""

from src.gold.bigquery.verificar_payload_local import verificar_sha256


def validar_preflight(
    filas,
    hash_declarado,
    version_silver,
    versiones_oficiales,
    autorizaciones_quality,
    producto,
    batch_id,
):
    if not isinstance(versiones_oficiales, dict):
        raise ValueError("Registro Silver oficial no disponible")

    if not isinstance(autorizaciones_quality, dict):
        raise ValueError("Autorizaciones Quality no disponibles")

    if not isinstance(producto, str) or not producto.strip():
        raise ValueError("Producto inválido")

    if not isinstance(batch_id, str) or not batch_id.strip():
        raise ValueError("Batch inválido")

    if not isinstance(version_silver, str) or not version_silver.strip():
        raise ValueError("Versión Silver inválida")

    version_acreditada = versiones_oficiales.get(producto)

    if version_acreditada != version_silver:
        raise PermissionError(
            "Versión Silver no acreditada como oficial"
        )

    autorizacion = autorizaciones_quality.get((producto, batch_id))

    if autorizacion != version_silver:
        raise PermissionError(
            "Autorización Quality ausente o incompatible"
        )

    verificar_sha256(filas, hash_declarado)

    return "PREFLIGHT_LOCAL_VALIDADO"


if __name__ == "__main__":
    from src.gold.bigquery.verificar_payload_local import calcular_sha256

    producto = "mercado_valor_diario"
    batch_id = "batch-prueba-001"
    version = "silver-simulada-v1"

    filas = [{
        "nemonico": "ABC",
        "fecha": "2026-11-27",
        "operation": "UPSERT",
        "valor": 100,
    }]

    huella = calcular_sha256(filas)

    try:
        validar_preflight(
            filas, huella, version, {}, {},
            producto, batch_id
        )
    except PermissionError:
        print("SIN_AUTORIZACION_BLOQUEADO_OK")
    else:
        raise AssertionError("Se permitió una carga no autorizada")

    # Datos exclusivamente simulados para comprobar la lógica.
    versiones_simuladas = {producto: version}
    autorizaciones_simuladas = {(producto, batch_id): version}

    resultado = validar_preflight(
        filas, huella, version,
        versiones_simuladas,
        autorizaciones_simuladas,
        producto, batch_id
    )

    assert resultado == "PREFLIGHT_LOCAL_VALIDADO"
    print("PREFLIGHT_SIMULADO_VALIDADO_OK")

    try:
        validar_preflight(
            [{**filas[0], "valor": 999}],
            huella,
            version,
            versiones_simuladas,
            autorizaciones_simuladas,
            producto,
            batch_id,
        )
    except ValueError:
        print("HASH_ALTERADO_BLOQUEADO_OK")
    else:
        raise AssertionError("Se aceptó un payload alterado")

    print("CONTROL_PREVIO_LOCAL_COMPLETADO_OK")
