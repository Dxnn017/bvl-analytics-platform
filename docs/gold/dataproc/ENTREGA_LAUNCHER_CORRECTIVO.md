# Launcher correctivo Gold supervisado - Fase 6

## Estado

Candidato tecnico local para revision de Lizeth y Quality.
No autorizado para ejecucion cloud ni publicacion.

Base tecnica ratificada: dc380e1.

## Objetivo

Preparar una ejecucion correctiva supervisada para:

- mercado_valor_diario: 41 columnas, 83130 filas esperadas.
- actividad_sab_diaria: 41 columnas, 21911 filas esperadas.
- empresa_trimestre: 58 columnas, 5158 filas esperadas.

Conservar por separado 85 excepciones de RUC y 6
colisiones financieras BNB.

Los conteos corresponden al alcance historico conciliado.
No son resultados de una nueva ejecucion GCP.

## Componentes nuevos

- autorizacion_correctiva_quality.py
- preparar_candidatos_correctivos.py
- escritura_correctiva_staging.py
- launcher_correctivo_supervisado.py

Los constructores y contratos Gold ratificados permanecen
sin modificaciones.

## Controles implementados

- Autorizacion correctiva ECDSA y huella oficial Quality.
- Validacion de vigencia y alcance.
- Vinculacion de autorizaciones Silver y contratos fisicos.
- Reutilizacion de constructores existentes.
- Incorporacion condicionada del sidecar SAB.
- Decision expresa para habilitar Empresa Trimestre.
- Comparacion nominal mediante SHA-256.
- Separacion de excepciones financieras.
- Validacion de columnas, tipos, claves y conteos.
- Materializacion temporal mediante PyArrow en driver.
- Creacion exclusiva GCS con if_generation_match=0.
- Sin reintentos automaticos de escritura GCS.
- Verificacion posterior de bytes y generaciones.
- Inventarios, auditorias y marcadores de ejecucion.
- Bloqueo de Hechos e Impacto.
- Sin publicacion automatica ni eliminacion de objetos.

## Evidencias focalizadas

- 20 pruebas de seguridad aprobadas.
- 8 pruebas de integracion funcional aprobadas.
- Spark local y datos sinteticos.
- GCS simulado.
- Firmas de prueba con claves efimeras.
- Sin repeticion de pruebas historicas.
- Sin nueva conciliacion financiera.
- Sin ejecucion GCP.

Las pruebas funcionales no recorren integralmente las seis
fuentes firmadas con los tres constructores reales.

## Contrato candidato de autorizacion

BVL-GOLD-CORRECTIVE-STAGING-CANDIDATE-v1.

Debe ser ratificado y firmado por Quality.

La autorizacion correctiva no sustituye las autorizaciones
Silver v0.3 vigentes de cada fuente.

La firma debe vincular el commit, SHA-256 del paquete y
launcher, identidad, alcance, contratos, destino staging,
ventana de ejecucion y decision financiera.

## Restricciones financieras

Version Silver conciliada:

v01-bvl-bronze-inicial-20261125-001-principales-cuentas-4cd3944afbbf413e

- Total historico: 5249.
- RUC no conciliados: 85.
- Colisiones BNB: 6.
- Aceptados: 5158.

Formato de huella nominal:
identificadores ordenados lexicograficamente, UTF-8,
separados por LF y con LF final.

Las huellas deben incorporarse a la decision Quality
firmada. Su generacion local no concede permisos.

## Staging propuesto

gs://bvl26-gold-7f3a9c/staging/correccion_fase6/
  batch_id=<batch>/
  run_id=<run>/
  attempt_id=<attempt>/

Subdirectorios:

- candidatos/
- calidad/
- _control/

No se escribira en versions, backups, current.json
ni registros oficiales.

Un intento parcial no se reutiliza automaticamente.
La existencia de COMPLETE.json no autoriza publicacion.

## Condiciones pendientes antes de ejecutar en GCP

1. Ratificacion del contrato correctivo por Quality.
2. Autorizaciones Silver nuevas y vigentes para las
   seis fuentes.
3. Contratos fisicos exactos y firmemente vinculados.
4. Sidecar SAB acreditado, si corresponde.
5. Decision financiera expresa para staging.
6. Acreditacion de identidad efectiva del proceso y
   su pertenencia al batch autorizado.
7. Confirmacion de IAM y destino staging exclusivo.
8. Confirmacion de dependencias y runtime Dataproc.
9. Validacion integral del flujo de las seis fuentes
   con los constructores y contratos efectivos.
10. Revision independiente de inventarios y generaciones.

## Limitaciones

- La consulta del batch por API no demuestra por si sola
  la pertenencia efectiva del proceso al batch consultado.
- El control de origen de modulos es complementario,
  no sustituye un entorno de despliegue confiable.
- La escritura multiobjeto no es atomica.
- Quality debe verificar el inventario y marcador final.
- No se ha probado la escritura GCS real.
- No se ha acreditado la integracion distribuida.
- El limite de bytes comprimidos no garantiza un limite
  de memoria RAM.
- Los lotes de 2048 filas no limitan necesariamente
  la memoria de cada particion Spark.
- Se mantiene pendiente la compatibilidad efectiva
  de dependencias del runtime.
- El entorno local utilizo cryptography 50.0.2,
  mientras el rango candidato previo es >=43,<50.
- ResourceWarning registrado durante pruebas locales.
- No se autorizan publicaciones ni nuevos batches.

## Veredicto

Candidato local entregable para revision tecnica.

No constituye conformidad productiva ni autorizacion
para iniciar la ejecucion correctiva.
