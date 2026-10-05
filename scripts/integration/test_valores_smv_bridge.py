import requests
import xml.etree.ElementTree as ET
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ============================================================
# CONFIGURACION
# ============================================================

URL = (
    "http://mvnet.smv.gob.pe/SMV.OpenData.wsGen/"
    "SMV.ServiciosOpenData.WCF.ServiceValores.svc"
)

SOAP_ACTION = (
    "http://tempuri.org/"
    "IServiceValores/consultaValoresRUC"
)

RUC_PRUEBA = {
    "20100027292": "FERREYCORP S.A.A.",
    "20100030595": "BANCO DE LA NACION",
    "20100041953": "RIMAC SEGUROS Y REASEGUROS",
}

COTIZACIONES = (
    "/datalake/bvl/silver/"
    "mercado_diario/cotizaciones"
)

HEADERS = {
    "Content-Type": "text/xml; charset=utf-8",
    "SOAPAction": f'"{SOAP_ACTION}"'
}


# ============================================================
# FUNCIONES SOAP
# ============================================================

def consultar_valores(ruc):

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

    r = requests.post(
        URL,
        data=body.encode("utf-8"),
        headers=HEADERS,
        timeout=60
    )

    r.raise_for_status()

    root = ET.fromstring(r.content)

    registros = []

    for elem in root.iter():

        if elem.tag.split("}")[-1] != "ValorObtenidoBE":
            continue

        registro = {}

        for child in elem:
            nombre = child.tag.split("}")[-1]

            valor = (
                child.text.strip()
                if child.text
                else ""
            )

            registro[nombre] = valor

        registros.append(registro)

    return registros


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("Validacion-Puente-SMV-Valores")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

cot = spark.read.parquet(COTIZACIONES)

valores_cotizados = {
    r["valor"]
    for r in (
        cot
        .select(
            F.upper(
                F.trim(F.col("valor"))
            ).alias("valor")
        )
        .filter(
            F.col("valor").isNotNull()
        )
        .distinct()
        .collect()
    )
}

print("=" * 80)
print("VALIDACION PUENTE SMV: RUC -> NEMONICO -> COTIZACIONES")
print("=" * 80)

print(
    "Valores distintos en Cotizaciones:",
    f"{len(valores_cotizados):,}"
)


# ============================================================
# PRUEBAS
# ============================================================

for ruc, empresa_esperada in RUC_PRUEBA.items():

    print()
    print("=" * 80)
    print("RUC:", ruc)
    print("Empresa esperada:", empresa_esperada)
    print("=" * 80)

    try:

        registros = consultar_valores(ruc)

    except Exception as e:

        print("ERROR CONSULTANDO SMV:")
        print(e)
        continue

    print(
        "Valores devueltos por SMV:",
        len(registros)
    )

    nemonicos = sorted({
        r.get("NemonicoValor", "").strip().upper()
        for r in registros
        if r.get("NemonicoValor", "").strip()
    })

    coincidencias = [
        n
        for n in nemonicos
        if n in valores_cotizados
    ]

    print(
        "Nemonicos distintos:",
        len(nemonicos)
    )

    print(
        "Coinciden con Cotizaciones:",
        len(coincidencias)
    )

    print()
    print("NEMONICOS SMV")

    for n in nemonicos[:30]:

        estado = (
            "MATCH"
            if n in valores_cotizados
            else "SIN MATCH"
        )

        print(
            f"  {n:<25} {estado}"
        )

    print()
    print("COINCIDENCIAS")

    if coincidencias:

        for n in coincidencias:
            print("  -", n)

    else:
        print("  Ninguna")


print()
print("=" * 80)
print("FIN VALIDACION")
print("=" * 80)

spark.stop()
