"""Verificación criptográfica local de firmas Quality.

Contrato candidato. No autoriza consumo ni publicación Gold.
La clave pública debe obtenerse de una fuente confiable externa.
"""

from pathlib import Path


class FirmaQualityError(ValueError):
    pass


def verificar_firma_ecdsa(
    contenido,
    firma,
    clave_publica_pem,
):
    """Verifica ECDSA-SHA256 sobre bytes exactos del documento."""
    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import ec
        from cryptography.exceptions import InvalidSignature
    except ImportError as error:
        raise FirmaQualityError(
            "Falta la dependencia cryptography."
        ) from error

    if not all(
        isinstance(valor, bytes)
        for valor in (contenido, firma, clave_publica_pem)
    ):
        raise FirmaQualityError(
            "Contenido, firma y clave deben ser bytes."
        )

    try:
        clave = serialization.load_pem_public_key(
            clave_publica_pem
        )

        if not isinstance(clave, ec.EllipticCurvePublicKey):
            raise FirmaQualityError(
                "La clave pública no es ECDSA."
            )

        clave.verify(
            firma,
            contenido,
            ec.ECDSA(hashes.SHA256()),
        )

    except InvalidSignature as error:
        raise FirmaQualityError(
            "Firma ECDSA inválida."
        ) from error

    except (ValueError, TypeError) as error:
        raise FirmaQualityError(
            "Clave pública o firma incompatible."
        ) from error

    return True


def verificar_archivo_firmado(
    archivo,
    archivo_firma,
    archivo_clave_publica,
):
    """Verifica archivos locales sin alterar su contenido."""
    return verificar_firma_ecdsa(
        Path(archivo).read_bytes(),
        Path(archivo_firma).read_bytes(),
        Path(archivo_clave_publica).read_bytes(),
    )
