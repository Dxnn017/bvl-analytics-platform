# Integracion incremental Gold a BigQuery

## Estado

CANDIDATO LOCAL NO AUTORIZADO.

Base de integracion historica:
7c77be9c0b8e71e3b7b8b2473a85700bd4f49c7b

Base Gold v3 ratificada:
62e0f0f76743822bc19859459e4307b2fb9b6ef2

## Objetivo

Consumir los deltas generados por el procesamiento
incremental Gold existente y preparar su transferencia
a BigQuery sin modificar los constructores ratificados.

## Productos

| Producto | Columnas Gold | Columnas staging esperadas |
|---|---:|---:|
| mercado_valor_diario | 41 | 47 |
| actividad_sab_diaria | 41 | 41+6 |
| empresa_trimestre | 58 | 64 |
| Total | 140 | 158 |

Claves:

- Mercado: nemonico, fecha.
- SAB: fecha, codigo_sab.
- Empresa: id_registro.

## Prerrequisitos operativos

- Carga historica inicial aprobada.
- Version Gold anterior aplicada en BigQuery.
- Nueva version Gold oficialmente aprobada.
- Manifest y firmas acreditados.
- Delta vinculado a las versiones correspondientes.
- Autorizacion incremental independiente.
- Identidad efectiva del ejecutor acreditada.
- Exclusividad de escritura y control concurrente.
- Inventario de transferencia protegido.

## Flujo

1. Recibir referencia al delta Gold aprobado.
2. Validar origen, version anterior y version nueva.
3. Verificar contrato, inventario, conteos y hashes.
4. Validar operaciones y claves.
5. Validar columnas obligatorias y tipos.
6. Aplicar conversion decimal exacta SAB.
7. Preparar las columnas de control staging.
8. Validar integridad del lote.
9. Ejecutar MERGE autorizado y transaccional.
10. Conciliar resultados y registrar la version.
11. Confirmar el lote completo.

## Operaciones

UPSERT:
Insertar claves inexistentes y actualizar las existentes.

DELETE:
Bloqueado en este alcance, incluso si el delta
contiene indicadores locales de autorizacion.

La ausencia de un registro en el delta no autoriza
eliminarlo de BigQuery.

Las operaciones emitidas por Gold deben contrastarse
con la interfaz real antes de implementar adaptadores.

No inventar formatos de delta ni valores de metadatos.

## Controles staging

- operation: STRING.
- delete_authorized: BOOL.
- batch_id: STRING.
- payload_sha256: STRING.
- run_id: STRING.
- source_version: STRING.

Estas columnas deben vincularse a los esquemas
staging existentes y conservar su obligatoriedad.

La definicion exacta de payload_sha256 debe respetar
su semantica existente en el proyecto.

## Precision

Conservar los tipos Gold.

Para las tres columnas SAB Decimal(34,2),
utilizar la politica de transferencia exacta
Decimal(31,2) desarrollada en la preparacion
historica, previa validacion de rango.

No aplicar redondeos ni reemplazos por NULL.

## Replay y continuidad

- Mismo lote y mismo contenido: no duplicar.
- Mismo lote con contenido distinto: rechazar.
- Version anterior incompatible: bloquear.
- Eventos atrasados o fuera de orden: bloquear
  hasta resolver la continuidad de versiones.
- Delta vacio: tratamiento explicito y acreditado.

No avanzar el registro de version si falla
la incorporacion del lote.

## Restricciones

No modificar Gold v3.
No modificar contratos existentes.
No repetir pruebas anteriores.
No ejecutar GCP ni BigQuery.
No conceder permisos.
No realizar operaciones DELETE.
No publicar datos.

## Alcance actual

Diseno candidato y vinculacion de interfaces.

La compatibilidad funcional entre los deltas reales,
el preparador incremental y MERGE se implementara
y validara en los siguientes pasos.

El inventario local no acredita una autorizacion
operativa ni la procedencia oficial de los datos.
