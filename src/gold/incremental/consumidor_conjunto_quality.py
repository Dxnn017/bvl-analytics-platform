"""Control conjunto Silver–Gold para pruebas locales.

Exige marcador firmado y referencias exactas de ambos productos.
No autoriza consumo productivo.
"""

from validar_marcador_conjunto_v2 import validar_marcador_firmado


class ConsumoConjuntoError(ValueError):
    pass


def validar_consumo_conjunto(
    marcador_bytes,
    firma_marcador,
    clave_publica_confiable,
    referencias_productos,
    autorizaciones,
):
    validacion = validar_marcador_firmado(
        marcador_bytes,
        firma_marcador,
        clave_publica_confiable,
    )

    versiones = validacion["versiones"]
    productos = {v["producto"] for v in versiones}

    if not isinstance(referencias_productos, dict):
        raise ConsumoConjuntoError("Referencias inválidas.")

    if set(referencias_productos) != productos:
        raise ConsumoConjuntoError("Faltan referencias conjuntas.")

    if not isinstance(autorizaciones, dict):
        raise ConsumoConjuntoError("Autorizaciones inválidas.")

    if set(autorizaciones) != productos:
        raise ConsumoConjuntoError("Autorización conjunta incompleta.")

    for version in versiones:
        producto = version["producto"]
        referencias = referencias_productos[producto]
        autorizacion = autorizaciones[producto]

        if not isinstance(referencias, dict):
            raise ConsumoConjuntoError("Referencias inválidas.")

        if not isinstance(autorizacion, dict):
            raise ConsumoConjuntoError("Autorización inválida.")

        if autorizacion.get("autorizacion_gold") is not True:
            raise ConsumoConjuntoError("Consumo Gold no autorizado.")

        if autorizacion.get("producto") != producto:
            raise ConsumoConjuntoError("Producto no autorizado.")

        if autorizacion.get("version_id") != version["version_id"]:
            raise ConsumoConjuntoError("Versión no autorizada.")

        for tipo in ("current", "manifest"):
            referencia = referencias.get(tipo)
            esperada = version[tipo]

            if not isinstance(referencia, dict):
                raise ConsumoConjuntoError("Referencia ausente.")

            for campo in ("uri", "generacion", "sha256"):
                if referencia.get(campo) != esperada[campo]:
                    raise ConsumoConjuntoError(
                        f"Referencia incompatible: {producto}/{tipo}/{campo}"
                    )

    return {
        "marcador_conjunto_verificado": True,
        "publicacion_conjunta_id": validacion["publicacion_conjunta_id"],
        "productos_validados": sorted(productos),
        "consumo_productivo_autorizado": False,
    }
