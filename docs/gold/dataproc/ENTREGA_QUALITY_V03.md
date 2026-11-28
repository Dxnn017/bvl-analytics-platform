# Entrega consolidada Gold - Quality v0.3

## 1. Objetivo

Integrar los mecanismos existentes de autorizacion Quality
v0.3 con el ejecutor portable Gold para Dataproc Serverless.

Alcance limitado al cierre tecnico candidato de la Fase 6.

No se autoriza consumo productivo, escritura GCS,
ejecucion BigQuery ni publicacion Gold.

## 2. Commits antecedentes

- 308e063: entrega candidata Fase 6.
- b828c74: compatibilidad Gold - BigQuery.
- 960ab64: control de versiones y replay.
- ed7b069: preparacion Dataproc Serverless.

## 3. Priorizacion acordada

| Producto | Estado |
|---|---|
| mercado_valor_diario | Prioritario para Quality |
| actividad_sab_diaria | Condicionado a cobertura |
| empresa_trimestre | Pendiente de conciliacion financiera |
| hechos_empresa | Bloqueado |
| impacto_hechos_valor | Bloqueado |

## 4. Integracion implementada

Modulo principal:

    src/gold/dataproc/integracion_quality_v03.py

Punto de entrada:

    src/gold/dataproc/main_gold.py

Funciones:

- verificar_fuentes_quality
- verificar_vigencia
- materializar_fuente_verificada
- preparar_candidato_quality
- preparar_preflight_candidato

Componentes reutilizados:

- flujo_silver_gold_v03.py
- autorizacion_gold_quality_v03.py
- verificar_conjunto_silver_v03.py
- control_concurrencia_gcs.py
- reconstruir_particiones_hive.py
- ejecutor_portable.py

No se modificaron los constructores Gold ratificados
durante esta integracion.

## 5. Flujo candidato

1. Validar producto permitido.
2. Comprobar dependencias Silver completas.
3. Verificar autorizaciones Quality firmadas.
4. Verificar manifiestos y registros por generacion.
5. Validar integridad SHA-256.
6. Validar esquemas, particiones y claves globales.
7. Recuperar bytes de los archivos acreditados.
8. Materializar DataFrames desde esos bytes.
9. Reutilizar el constructor Gold correspondiente.
10. Comprobar nuevamente vigencia de autorizaciones.
11. Devolver candidato en memoria y reporte.
12. Mantener publicacion bloqueada.

No se realiza lectura Spark abierta por prefijos GCS.

## 6. Evidencias nuevas

- 6 pruebas unitarias de integracion aprobadas.
- 3 pruebas fisicas Parquet y Spark aprobadas.
- Reconstruccion de particiones Hive correcta.
- Rechazo de bytes alterados.
- Rechazo de conteos incompatibles.
- Bloqueo de CLI cloud antes de iniciar Spark.
- Importacion aislada desde paquete ZIP.
- Paquete actualizado con 78 modulos.

Las pruebas de GCS utilizan objetos simulados.
El procesamiento del Parquet sintetico y Spark es local.

No se repitieron las pruebas anteriormente aprobadas.

## 7. Insumos pendientes

Responsabilidad de coordinacion y Quality:

- Referencias de las seis versiones Silver.
- Autorizaciones firmadas de consumo.
- Clave publica oficial acreditada.
- Generaciones y SHA-256 oficiales.
- Contratos fisicos de las fuentes.
- Rutas GCS, manifests y run_id.
- Evidencias de los tres candidatos Gold existentes.
- Inventario de 14 archivos SAB excluidos.
- Evidencia de 245 registros SAB en cuarentena.
- Conciliacion de 91 registros financieros.
- Subred y bucket autorizado para dependencias.

## 8. Limitaciones

- El modo preflight-quality permanece bloqueado en CLI.
- La interfaz Python admite cliente y evidencias inyectados.
- No existe habilitacion cloud operativa automatica.
- No se ha ejecutado el flujo con datos oficiales GCS.
- El contrato fisico inyectado debe acreditarse oficialmente.
- La integracion Quality actual reconstruye candidatos
  completos en memoria.
- La integracion incremental con base Gold y alcance
  acreditados todavia no esta habilitada.
- El limite de 128 MiB corresponde a bytes comprimidos
  declarados, no a memoria maxima del driver.
- El limite se comprueba despues del preflight fisico.
- La cobertura SAB permanece desconocida sin sidecar
  acreditado para la version correspondiente.
- La atomicidad distribuida y el replay BigQuery real
  no estan acreditados.
- Se observo ResourceWarning de socket en Spark local,
  sin fallos de las pruebas.
- La aprobacion productiva sigue siendo responsabilidad
  de Quality.

## 9. Estado

Integracion candidata local implementada.
Validaciones focalizadas aprobadas.
Ejecucion cloud no realizada.
Publicacion productiva no autorizada.
Cierre oficial de Fase 6 pendiente de Quality.
