"""Conteos verificables contra estado local SQLite."""

import json
import sqlite3

from src.gold.bigquery.comparador_negocio_local import cambio_negocio

CLAVES = {
    "mercado_valor_diario": ("nemonico", "fecha"),
    "actividad_sab_diaria": ("fecha", "codigo_sab"),
    "empresa_trimestre": ("id_registro",),
}


def contar_cambios(conexion, producto, filas):
    if producto not in CLAVES:
        raise ValueError("Producto desconocido")

    insertados = 0
    actualizados = 0
    sin_cambios = 0
    vistos = set()

    for fila in filas:
        if fila.get("operation") != "UPSERT":
            raise ValueError("Operacion no autorizada")

        clave = tuple(fila.get(c) for c in CLAVES[producto])

        if any(v is None or v == "" for v in clave):
            raise ValueError("Clave invalida")

        clave_json = json.dumps(clave, ensure_ascii=False)

        if clave_json in vistos:
            raise ValueError("Clave duplicada")

        vistos.add(clave_json)

        contenido = json.dumps(
            fila, sort_keys=True, ensure_ascii=False
        )

        anterior = conexion.execute(
            """
            SELECT contenido
            FROM datos
            WHERE product = ? AND clave = ?
            """,
            (producto, clave_json),
        ).fetchone()

        if anterior is None:
            insertados += 1
        elif cambio_negocio(
            producto, json.loads(anterior[0]), fila
        ):
            actualizados += 1
        else:
            sin_cambios += 1

    return {
        "inserted_rows": insertados,
        "updated_rows": actualizados,
        "unchanged_rows": sin_cambios,
        "deleted_rows": 0,
    }


if __name__ == "__main__":
    from src.gold.bigquery.adaptador_incremental_local import aplicar_batch

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
        db = sqlite3.connect(":memory:")

        fila = {
            **clave,
            "operation": "UPSERT",
            "valor": 100,
        }

        campo_medida = {
            "mercado_valor_diario": "precio_cierre",
            "actividad_sab_diaria": "monto_total",
            "empresa_trimestre": "activo_total",
        }[producto]

        fila[campo_medida] = fila.pop("valor")

        aplicar_batch(
            db, producto, "batch-1", "run-1", "v1", [fila]
        )

        nueva = {
            **clave,
            "valor": 200,
        }

        nueva[campo_medida] = nueva.pop("valor")

        otra = dict(nueva)
        ultima_clave = list(CLAVES[producto])[-1]
        otra[ultima_clave] = "NUEVO"

        cambios = [
            fila,
            {**nueva, "operation": "UPSERT"},
            {**otra, "operation": "UPSERT"},
        ]

        # No incluir dos veces una misma clave en un batch.
        # Se prueban escenarios por separado.
        igual = contar_cambios(db, producto, [fila])

        actualizado = contar_cambios(
            db, producto,
            [{**nueva, "operation": "UPSERT"}],
        )

        nuevo = contar_cambios(
            db, producto,
            [{**otra, "operation": "UPSERT"}],
        )

        assert igual["unchanged_rows"] == 1
        assert actualizado["updated_rows"] == 1
        assert nuevo["inserted_rows"] == 1

        print(f"{producto}: CONTEOS_INCREMENTALES_OK")
        db.close()

    print("TRES_PRODUCTOS_CON_CONTEOS_VERIFICADOS_OK")
