# Adaptacion acreditada de Cotizaciones

## Antecedente

Commit candidato aceptado:
f78a44f1fdbf23a8ea469c9bed3d350f3d97d729.

Quality identifico que el Silver oficial de
mercado_diario/cotizaciones no contiene fuente
ni fecha_ingesta.

Quality admite expresamente NULL tipado porque
no existe evidencia acreditada suficiente para
recuperar dichos metadatos por registro.

## Solucion

Adaptador exclusivo de Cotizaciones:

- fuente: NULL string.
- fecha_ingesta: NULL date.
- Conservacion de todas las columnas originales.
- Sin modificacion del Silver oficial.
- Sin sobrescritura de metadatos.
- Sin modificacion de constructores ratificados.
- Sin modificacion de contratos Gold.

La adaptacion se aplica despues de verificar
y materializar Silver y antes del constructor
de Mercado Valor Diario.

## Autorizacion Quality

Nuevo esquema candidato:

BVL-GOLD-CORRECTIVE-STAGING-CANDIDATE-v2

Campo obligatorio:

cotizaciones_adaptation

La decision debe formar parte del documento
completo firmado por Quality.

Se verifica:

- Identificador de decision.
- Aprobacion expresa.
- Producto exacto.
- Version Silver exacta.
- Justificacion acreditada.
- Columnas y tipos exactos.
- Valores exclusivamente NULL.

Las autorizaciones v1 no habilitan la nueva
adaptacion.

## Auditoria

La evidencia de adaptacion se registra en
_control/AUDIT.json e incluye:

- Identificador de autorizacion.
- Decision Quality.
- Producto y version Silver.
- Referencia al manifest.
- Conteo acreditado.
- Columnas de entrada y salida.
- Tipos y valores incorporados.
- Indicadores de no modificacion de Silver.
- Publicacion no autorizada.

La escritura queda bloqueada si falta la
evidencia requerida de adaptacion.

## Evidencias

10 pruebas focalizadas aprobadas.
Tiempo: 16.225 segundos.

Spark local y almacenamiento simulado.
Constructores simulados en la prueba de
integracion del adaptador.

No se repitieron las pruebas historicas.

Las 28 pruebas anteriores corresponden
al commit candidato precedente, no a una
reejecucion sobre esta revision.

## Alcance pendiente

La integracion operativa de las seis fuentes
Silver oficiales, los constructores reales,
las autorizaciones firmadas y el runtime
Dataproc corresponde a la revision
independiente coordinada por Lizeth.

No se ha ejecutado GCP.
No se ha modificado Silver oficial.
No se han modificado permisos.
No se ha autorizado publicacion oficial.

Entrega candidata para revision operativa.
