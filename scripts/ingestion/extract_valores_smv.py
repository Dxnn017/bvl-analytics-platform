import csv
import json
import time
import requests
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime

# ============================================================
# CONFIGURACION
# ============================================================

BASE = Path("/media/sf_bvl_shared/puentes_smv")

RUC_FILE = BASE / "rucs_principales_cuentas.csv"

RAW_DIR = BASE / "raw_valores_smv"

OUT_JSON = BASE / "valores_inscritos_smv.json"
OUT_CSV = BASE / "valores_inscritos_smv.csv"
OUT_LOG = BASE / "extraccion_valores_smv_log.csv"

URL = (
    "http://mvnet.smv.gob.pe/"
    "SMV.OpenData.wsGen/"
    "SMV.ServiciosOpenData.WCF.ServiceValores.svc"
)

SOAP_ACTION = (
    "http://tempuri.org/"
    "IServiceValores/consultaValoresRUC"
)

HEADERS = {
    "Content-Type": "text/xml; charset=utf-8",
    "SOAPAction": f'"{SOAP_ACTION}"'
}

RAW_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# UTILIDADES
# ============================================================

def limpiar(valor):
    if valor is None:
        return ""
    return str(valor).strip()


def consultar_ruc(session, ruc):

    body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope
 xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
 xmlns:tem="http://tempuri.org/">
 <soap:Body>
  <tem:consultaValoresRUC>
   <tem:pstrRUC>{ruc}</tem:pstrRUC>
  </tem:consultaValoresRUC>
 </soap:Body>
</soap:Envelope>
"""

    respuesta = session.post(
        URL,
        data=body.encode("utf-8"),
        headers=HEADERS,
        timeout=60
    )

    respuesta.raise_for_status()

    return respuesta


def parsear_respuesta(xml_bytes, ruc_consultado):

    root = ET.fromstring(xml_bytes)

    registros = []

    for elem in root.iter():

        if elem.tag.split("}")[-1] != "ValorObtenidoBE":
            continue

        registro = {
            "RUC": ruc_consultado
        }

        for child in elem:

            nombre = child.tag.split("}")[-1]

            registro[nombre] = limpiar(
                child.text
            )

        registros.append(registro)

    return registros


# ============================================================
# LEER UNIVERSO DE RUC
# ============================================================

with open(
    RUC_FILE,
    "r",
    encoding="utf-8-sig",
    newline=""
) as f:

    universo = list(
        csv.DictReader(f)
    )

print("=" * 80)
print("EXTRACCION MASIVA - VALORES INSCRITOS SMV")
print("=" * 80)

print("RUC a consultar :", len(universo))
print("Servicio        :", URL)
print("Directorio RAW  :", RAW_DIR)

# ============================================================
# EXTRACCION
# ============================================================

session = requests.Session()

todos = []
logs = []

for i, empresa in enumerate(universo, start=1):

    ruc = limpiar(empresa.get("ruc"))
    nombre_origen = limpiar(
        empresa.get("nombre_empresa")
    )

    raw_file = RAW_DIR / f"{ruc}.xml"

    print()
    print(
        f"[{i}/{len(universo)}] "
        f"{ruc} | {nombre_origen}"
    )

    estado = ""
    error = ""
    cantidad = 0

    try:

        # ----------------------------------------------------
        # REUTILIZAR RESPUESTA SI YA EXISTE
        # ----------------------------------------------------

        if raw_file.exists():

            xml_bytes = raw_file.read_bytes()

            estado = "REUTILIZADO"

        else:

            respuesta = consultar_ruc(
                session,
                ruc
            )

            xml_bytes = respuesta.content

            raw_file.write_bytes(
                xml_bytes
            )

            estado = "DESCARGADO"

            # pequeña pausa para no saturar el servicio
            time.sleep(0.30)

        registros = parsear_respuesta(
            xml_bytes,
            ruc
        )

        cantidad = len(registros)

        for registro in registros:

            registro["_ruc_consultado"] = ruc
            registro["_empresa_origen"] = nombre_origen
            registro["_fuente"] = "SMV ServiceValores"
            registro["_fecha_extraccion"] = (
                datetime.now()
                .isoformat(timespec="seconds")
            )

            todos.append(registro)

        print(
            f"  {estado} | valores: {cantidad}"
        )

    except Exception as e:

        estado = "ERROR"
        error = str(e)

        print(
            "  ERROR:",
            error
        )

    logs.append({
        "ruc": ruc,
        "empresa_origen": nombre_origen,
        "estado": estado,
        "cantidad_valores": cantidad,
        "error": error
    })


# ============================================================
# GUARDAR JSON
# ============================================================

with open(
    OUT_JSON,
    "w",
    encoding="utf-8"
) as f:

    json.dump(
        {
            "fuente": (
                "Superintendencia del "
                "Mercado de Valores - SMV"
            ),
            "servicio": (
                "consultaValoresRUC"
            ),
            "endpoint": URL,
            "fecha_extraccion": (
                datetime.now()
                .isoformat(timespec="seconds")
            ),
            "ruc_consultados": len(universo),
            "registros": len(todos),
            "Resultado": todos
        },
        f,
        ensure_ascii=False,
        indent=2
    )


# ============================================================
# GUARDAR CSV
# ============================================================

columnas = [
    "RUC",
    "RazonSocial",
    "DenominacionValor",
    "NemonicoValor",
    "CodigoISIN",
    "TipoValor",
    "FechaInscripcion",
    "ResolucionInscripcion",
    "Moneda",
    "MontoInscrito",
    "Cotizacion",
    "FechaUltCot",
    "_ruc_consultado",
    "_empresa_origen",
    "_fuente",
    "_fecha_extraccion"
]

with open(
    OUT_CSV,
    "w",
    encoding="utf-8-sig",
    newline=""
) as f:

    writer = csv.DictWriter(
        f,
        fieldnames=columnas,
        extrasaction="ignore"
    )

    writer.writeheader()

    for r in todos:
        writer.writerow(r)


# ============================================================
# LOG
# ============================================================

with open(
    OUT_LOG,
    "w",
    encoding="utf-8-sig",
    newline=""
) as f:

    writer = csv.DictWriter(
        f,
        fieldnames=[
            "ruc",
            "empresa_origen",
            "estado",
            "cantidad_valores",
            "error"
        ]
    )

    writer.writeheader()
    writer.writerows(logs)


# ============================================================
# RESUMEN
# ============================================================

con_valores = sum(
    1
    for r in logs
    if r["cantidad_valores"] > 0
)

sin_valores = sum(
    1
    for r in logs
    if (
        r["cantidad_valores"] == 0
        and r["estado"] != "ERROR"
    )
)

errores = sum(
    1
    for r in logs
    if r["estado"] == "ERROR"
)

nemonicos = {
    limpiar(r.get("NemonicoValor")).upper()
    for r in todos
    if limpiar(r.get("NemonicoValor"))
}

isins = {
    limpiar(r.get("CodigoISIN")).upper()
    for r in todos
    if limpiar(r.get("CodigoISIN"))
}

print()
print("=" * 80)
print("RESUMEN FINAL")
print("=" * 80)

print(
    "RUC consultados       :",
    len(universo)
)

print(
    "RUC con valores       :",
    con_valores
)

print(
    "RUC sin valores       :",
    sin_valores
)

print(
    "RUC con error         :",
    errores
)

print(
    "Registros obtenidos   :",
    len(todos)
)

print(
    "Nemonicos distintos   :",
    len(nemonicos)
)

print(
    "ISIN distintos        :",
    len(isins)
)

print()
print("JSON:")
print(OUT_JSON)

print()
print("CSV:")
print(OUT_CSV)

print()
print("LOG:")
print(OUT_LOG)

print()
print("=" * 80)
print("EXTRACCION FINALIZADA")
print("=" * 80)
