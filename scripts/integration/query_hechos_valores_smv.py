import csv
import json
import re
import time
from pathlib import Path
from xml.sax.saxutils import escape
import xml.etree.ElementTree as ET

import requests


# ============================================================
# CONFIGURACION
# ============================================================

AUDIT = Path(
    "outputs/quality/"
    "hechos_empresa_bridge_audit.json"
)

BASE = Path(
    "/media/sf_bvl_shared/"
    "puentes_smv/hechos_valores_smv"
)

RAW_DIR = BASE / "raw_xml"

OUT_JSON = (
    BASE /
    "hechos_empresas_valores_smv.json"
)

OUT_CSV = (
    BASE /
    "hechos_empresas_valores_smv.csv"
)

OUT_SUMMARY = (
    BASE /
    "hechos_empresas_valores_smv_resumen.csv"
)

URL = (
    "http://mvnet.smv.gob.pe/"
    "SMV.OpenData.wsGen/"
    "SMV.ServiciosOpenData.WCF.ServiceValores.svc"
)

SOAP_ACTION = (
    "http://tempuri.org/"
    "IServiceValores/"
    "consultaValoresRAZONSOCIAL"
)

HEADERS = {
    "Content-Type": "text/xml; charset=utf-8",
    "SOAPAction": f'"{SOAP_ACTION}"'
}

RAW_DIR.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# UTILIDADES
# ============================================================

def local_name(tag):
    return tag.split("}", 1)[-1]


def nombre_archivo(nombre, indice):

    seguro = re.sub(
        r"[^A-Za-z0-9]+",
        "_",
        nombre
    ).strip("_")

    seguro = seguro[:80]

    return (
        f"{indice:02d}_"
        f"{seguro}.xml"
    )


def crear_envelope(razon_social):

    razon = escape(razon_social)

    return f"""<?xml version="1.0" encoding="utf-8"?>
<s:Envelope
    xmlns:s="http://schemas.xmlsoap.org/soap/envelope/">
  <s:Body>
    <consultaValoresRAZONSOCIAL
        xmlns="http://tempuri.org/">
      <pstrRAZONSOCIAL>{razon}</pstrRAZONSOCIAL>
    </consultaValoresRAZONSOCIAL>
  </s:Body>
</s:Envelope>
"""


def extraer_valores(xml_text):

    root = ET.fromstring(xml_text)

    registros = []

    for elem in root.iter():

        if local_name(elem.tag) != "ValorObtenidoBE":
            continue

        registro = {}

        for child in list(elem):

            nombre = local_name(
                child.tag
            )

            valor = (
                child.text.strip()
                if child.text
                else ""
            )

            registro[nombre] = valor

        registros.append(
            registro
        )

    # Capturar también posibles mensajes del servicio
    codigo_error = ""
    mensaje_error = ""

    for elem in root.iter():

        nombre = local_name(
            elem.tag
        )

        valor = (
            elem.text.strip()
            if elem.text
            else ""
        )

        if nombre == "CodigoError":
            codigo_error = valor

        elif nombre == "MensajeError":
            mensaje_error = valor

    return (
        registros,
        codigo_error,
        mensaje_error
    )


# ============================================================
# LEER EMPRESAS SIN MATCH
# ============================================================

if not AUDIT.exists():
    raise FileNotFoundError(
        f"No existe {AUDIT}"
    )

audit = json.loads(
    AUDIT.read_text(
        encoding="utf-8"
    )
)

empresas = audit.get(
    "primeras_empresas_sin_match",
    []
)

print("=" * 85)
print("CONSULTA OFICIAL SMV - HECHOS SIN MATCH")
print("=" * 85)

print(
    "Empresas a consultar:",
    len(empresas)
)

if len(empresas) != 22:
    print(
        "ADVERTENCIA: se esperaban 22 empresas."
    )


# ============================================================
# CONSULTAR SOAP
# ============================================================

session = requests.Session()

todos = []
resumen = []

for i, item in enumerate(
    empresas,
    start=1
):

    empresa = (
        item.get("empresa")
        or ""
    ).strip()

    eventos = int(
        item.get(
            "eventos",
            0
        )
    )

    print()
    print(
        f"[{i}/{len(empresas)}] "
        f"{empresa}"
    )

    envelope = crear_envelope(
        empresa
    )

    xml_path = (
        RAW_DIR /
        nombre_archivo(
            empresa,
            i
        )
    )

    try:

        response = session.post(
            URL,
            data=envelope.encode(
                "utf-8"
            ),
            headers=HEADERS,
            timeout=60
        )

        status = response.status_code

        xml_text = response.text

        xml_path.write_text(
            xml_text,
            encoding="utf-8"
        )

        if status != 200:

            print(
                "  HTTP:",
                status
            )

            resumen.append({
                "empresa_consultada": empresa,
                "eventos": eventos,
                "http_status": status,
                "valores": 0,
                "nemonicos_distintos": 0,
                "razones_sociales_devueltas": "",
                "codigo_error": "",
                "mensaje_error": (
                    "HTTP no exitoso"
                )
            })

            continue

        (
            valores,
            codigo_error,
            mensaje_error
        ) = extraer_valores(
            xml_text
        )

        nemonicos = sorted({
            (
                v.get(
                    "NemonicoValor"
                )
                or ""
            ).strip().upper()
            for v in valores
            if (
                v.get(
                    "NemonicoValor"
                )
                or ""
            ).strip()
        })

        razones = sorted({
            (
                v.get(
                    "RazonSocial"
                )
                or ""
            ).strip()
            for v in valores
            if (
                v.get(
                    "RazonSocial"
                )
                or ""
            ).strip()
        })

        print(
            "  HTTP       :",
            status
        )

        print(
            "  Valores    :",
            len(valores)
        )

        print(
            "  Nemonicos  :",
            len(nemonicos)
        )

        if razones:

            print(
                "  Razon SMV  :",
                " | ".join(
                    razones[:3]
                )
            )

        if nemonicos:

            print(
                "  Ejemplos   :",
                ", ".join(
                    nemonicos[:10]
                )
            )

        for valor in valores:

            registro = {
                "empresa_consultada":
                    empresa,

                "eventos_origen":
                    eventos,

                **valor
            }

            todos.append(
                registro
            )

        resumen.append({
            "empresa_consultada":
                empresa,

            "eventos":
                eventos,

            "http_status":
                status,

            "valores":
                len(valores),

            "nemonicos_distintos":
                len(nemonicos),

            "razones_sociales_devueltas":
                " | ".join(
                    razones
                ),

            "codigo_error":
                codigo_error,

            "mensaje_error":
                mensaje_error
        })

    except Exception as e:

        print(
            "  ERROR:",
            repr(e)
        )

        resumen.append({
            "empresa_consultada":
                empresa,

            "eventos":
                eventos,

            "http_status":
                "",

            "valores":
                0,

            "nemonicos_distintos":
                0,

            "razones_sociales_devueltas":
                "",

            "codigo_error":
                "",

            "mensaje_error":
                repr(e)
        })

    time.sleep(0.2)


# ============================================================
# GUARDAR JSON
# ============================================================

OUT_JSON.write_text(
    json.dumps(
        todos,
        ensure_ascii=False,
        indent=2
    ),
    encoding="utf-8"
)


# ============================================================
# GUARDAR CSV DETALLADO
# ============================================================

if todos:

    campos = [
        "empresa_consultada",
        "eventos_origen"
    ]

    adicionales = sorted({
        k
        for r in todos
        for k in r.keys()
        if k not in campos
    })

    campos += adicionales

    with OUT_CSV.open(
        "w",
        encoding="utf-8-sig",
        newline=""
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=campos
        )

        writer.writeheader()

        writer.writerows(
            todos
        )

else:

    OUT_CSV.write_text(
        "",
        encoding="utf-8"
    )


# ============================================================
# GUARDAR RESUMEN
# ============================================================

with OUT_SUMMARY.open(
    "w",
    encoding="utf-8-sig",
    newline=""
) as f:

    campos = [
        "empresa_consultada",
        "eventos",
        "http_status",
        "valores",
        "nemonicos_distintos",
        "razones_sociales_devueltas",
        "codigo_error",
        "mensaje_error"
    ]

    writer = csv.DictWriter(
        f,
        fieldnames=campos
    )

    writer.writeheader()

    writer.writerows(
        resumen
    )


# ============================================================
# RESUMEN FINAL
# ============================================================

con_valores = [
    r
    for r in resumen
    if int(
        r.get(
            "valores",
            0
        )
        or 0
    ) > 0
]

sin_valores = [
    r
    for r in resumen
    if int(
        r.get(
            "valores",
            0
        )
        or 0
    ) == 0
]

nemonicos = {
    (
        r.get(
            "NemonicoValor"
        )
        or ""
    ).strip().upper()
    for r in todos
    if (
        r.get(
            "NemonicoValor"
        )
        or ""
    ).strip()
}

print()
print("=" * 85)
print("RESUMEN FINAL")
print("=" * 85)

print(
    "Empresas consultadas :",
    len(empresas)
)

print(
    "Empresas con valores :",
    len(con_valores)
)

print(
    "Empresas sin valores :",
    len(sin_valores)
)

print(
    "Registros obtenidos  :",
    len(todos)
)

print(
    "Nemonicos distintos  :",
    len(nemonicos)
)

print()
print("JSON:")
print(OUT_JSON)

print()
print("CSV:")
print(OUT_CSV)

print()
print("RESUMEN:")
print(OUT_SUMMARY)

print()
print("=" * 85)
print("CONSULTA FINALIZADA")
print("=" * 85)
