"""Verificación SHA-256 local. No valida Parquet oficial."""

import hashlib
import json
import re


def calcular_sha256(filas):
    if not isinstance(filas, list) or not filas:
        raise ValueError("Payload vacío o inválido")

    contenido = json.dumps(
        filas,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(contenido.encode("utf-8")).hexdigest()


def verificar_sha256(filas, hash_declarado):
    if not isinstance(hash_declarado, str):
        raise ValueError("SHA-256 inválido")

    if not re.fullmatch(r"[0-9a-fA-F]{64}", hash_declarado):
        raise ValueError("Formato SHA-256 inválido")

    calculado = calcular_sha256(filas)

    if calculado.lower() != hash_declarado.lower():
        raise ValueError("SHA-256 no coincide con el contenido")

    return True


if __name__ == "__main__":
    casos = {
        "mercado_valor_diario": {
            "nemonico": "ABC", "fecha": "2026-11-27"
        },
        "actividad_sab_diaria": {
            "fecha": "2026-11-27", "codigo_sab": "S01"
        },
        "empresa_trimestre": {
            "id_registro": "REG-001"
        },
    }

    for producto, clave in casos.items():
        filas = [{**clave, "operation": "UPSERT", "valor": 100}]
        huella = calcular_sha256(filas)

        assert verificar_sha256(filas, huella)

        alteradas = [{**clave, "operation": "UPSERT", "valor": 999}]

        try:
            verificar_sha256(alteradas, huella)
        except ValueError:
            pass
        else:
            raise AssertionError("Payload alterado no rechazado")

        print(f"{producto}: SHA256_CONTENIDO_VALIDADO_OK")

    print("TRES_VERIFICACIONES_SHA256_LOCALES_OK")
