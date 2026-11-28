# Entrega integral Gold Fase 6 — v3

## Estado

CANDIDATA LOCAL PARA REVISION E INTEGRACION QUALITY.

No constituye una autorizacion de ejecucion,
escritura, consumo productivo ni publicacion.

Base de trabajo:
4d4adb9403086d7fe51aa4d11474c75591bbad47

Base de constructores preservada:
dc380e1f68973f83ca79bb2ffec53edeb68b4709

## Objetivo

Resolver conjuntamente las incompatibilidades
entre seis esquemas Silver candidatos oficiales
y los tres constructores Gold reales.

## Diagnostico

- Mercado: error de operacion aritmetica entre String.
- SAB: tres porcentajes permanecian como String.
- Empresa: 58 columnas compatibles sin adaptacion.

## Solucion integral

- Siete conversiones String a Double en Cotizaciones.
- Tres conversiones String a Double en porcentajes SAB.
- Tres conversiones String a Decimal(24,2) en montos SAB.
- Conservacion de los dos NULL tipados de Cotizaciones.
- Conservacion de NULL originales.
- Rechazo de cadenas ambiguas, formatos invalidos,
  valores no finitos y desbordamientos.
- Rechazo de redondeo monetario no autorizado.
- Autorizacion candidata Quality v3.
- Interpretacion del sidecar SAB original firmado y
  su ratificacion independiente.
- Conservacion de cobertura DESCONOCIDA.
- Verificacion de claves de negocio.
- Evidencia obligatoria de adaptaciones en auditoria.

## Resultados locales

| Control | Resultado |
|---|---|
| Validaciones de decision numerica | 10/10 |
| Validaciones documentales SAB | 8/8 |
| Casos numericos | 24/24 |
| Controles de auditoria | 18/18 |
| Constructores reales | 3/3 |
| Constructores simulados | 0 |
| Esquemas Silver | 6/6 |
| Mercado | 41/41 columnas |
| SAB | 41/41 columnas |
| Empresa | 58/58 columnas |
| Total Gold | 140/140 columnas |
| Roundtrip Parquet local | 3/3 |
| Fechas SAB sinteticas | 1207 |
| Registros SAB sinteticos | 21911 |

Los resultados de las 10 y 8 validaciones iniciales
fueron reportados en terminal durante la implementacion.
Los informes Spark y writer se adjuntan como evidencias.

## Productos candidatos

Conteos historicos esperados, no resultados de
una nueva ejecucion oficial:

- mercado_valor_diario: 83130.
- actividad_sab_diaria: 21911.
- empresa_trimestre: 5158.
- Total candidato: 110199.

Excepciones financieras separadas:

- RUC no conciliado: 85.
- RUC valido ambiguo: 6.

Se conserva la decision financiera de 91 excepciones.
No se reejecuto la conciliacion historica nominal.

## Sidecar SAB

Se conserva el documento original y su firma.
La ratificacion independiente debe verificarse
criptograficamente y vincularse a la version,
manifest, firma y generacion correspondientes.

El conteo de registros elegibles no acredita
cobertura completa de mercado.

No se utiliza el borrador operativo como
autorizacion.

## Condiciones previas a ejecucion supervisada

Quality debe:

1. Ratificar los seis contratos fisicos Silver.
2. Acreditar los tres contratos Gold.
3. Preparar autorizaciones Silver vigentes.
4. Acreditar las referencias GCS y generaciones.
5. Firmar una nueva autorizacion correctiva v3.
6. Vincular el commit, ZIP de modulos y launcher.
7. Acreditar las decisiones numericas, NULL tipados,
   financiera y sidecar, segun corresponda.
8. Verificar identidad efectiva y runtime del batch.
9. Confirmar expresamente una ejecucion supervisada.

Las referencias de sidecar y ratificacion deben
corresponder a documentos originales acreditados.

La politica numerica rechaza todo valor no nulo
que incumpla los formatos y rangos permitidos.

## Limitaciones

- No se ejecutaron GCP ni BigQuery.
- No se descargaron Parquet oficiales.
- Los datos de negocio de las pruebas son sinteticos.
- Los contratos fisicos siguen siendo candidatos.
- El preflight y la autorizacion utilizados en las
  pruebas Spark contienen fixtures locales.
- Las pruebas writer utilizaron transporte y
  materializacion simulados.
- La materializacion Parquet real se comprobo
  separadamente con Spark y PyArrow locales.
- No se ejecuto integralmente el launcher cloud.
- No se garantiza la convertibilidad de todos los
  valores Silver oficiales.
- Los conteos SAB diarios deben contrastarse contra
  los datos oficiales durante la ejecucion autorizada.
- Las suites anteriores no fueron repetidas.
- Los fixtures y ejemplos de autorizacion v1/v2
  anteriores no deben interpretarse como compatibles
  con la nueva autorizacion v3.
- No se acredita nuevamente el conjunto nominal
  financiero de los 5249 registros.
- La comprobacion del paquete no sustituye una
  cadena de confianza operativa del launcher.
- La identidad efectiva del proceso y su asociacion
  al batch deben acreditarse independientemente.

## Reproduccion local

Utilizar Python 3.11, Spark 3.5.1 y el entorno
de dependencias del proyecto.

Configurar PYTHONPATH con los directorios Gold
dataproc, incremental, mercado y empresas.

La prueba integral manual requiere ZIP_CONTRATOS
apuntando al ZIP original entregado por Quality.

Scripts:

- tests/gold/manual/validacion_constructores_reales_v3.py
- tests/gold/test_evidencia_integral_writer_v3.py

No es necesario repetir estas pruebas para
comprobar las huellas del paquete entregado.

## Restricciones

Silver oficial: intacto.
Constructores ratificados: intactos.
Contratos Gold: intactos.
Ejecucion GCP: no realizada.
Publicacion: no autorizada.

hechos_empresa e impacto_hechos_valor
permanecen bloqueados.
