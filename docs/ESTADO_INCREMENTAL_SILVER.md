# BVL Analytics - Pipeline incremental Silver

## 1. Estado de implementacion

Rama: pipeline-daniela

Componentes desarrollados:
- Deteccion incremental Bronze - Silver.
- Orquestacion y registro de lotes.
- Procesador BCRP V2 optimizado.
- Prevalidacion de publicacion.
- Publicador integrado de laboratorio.
- Registro persistente de transacciones en HDFS.
- Respaldo y recuperacion ante fallos simulados.
- Versionado de prueba con SHA-256.
- Paquete preliminar Silver - Gold.

## 2. Evidencias validadas

- 8/8 pruebas unitarias de seguridad aprobadas.
- 13 registros historicos y 14 publicados en prueba.
- 1 registro nuevo y 0 registros perdidos.
- Recuperacion tras dos puntos de interrupcion.
- Exclusión mutua comprobada con dos procesos.
- Verificacion SHA-256 de version experimental.
- 13 scripts con sintaxis Python valida.

## 3. Limitaciones actuales

- Silver oficial no fue modificado en las pruebas.
- Las publicaciones realizadas son de laboratorio.
- Tres scripts contienen referencias a /tmp.
- Una prueba utiliza un RUN_ID fijo.
- El versionado experimental no garantiza
  inmutabilidad mediante permisos.
- Falta consolidar la publicacion oficial segura.
- Falta completar la recuperacion ante todos
  los estados de fallo y el control de lectores.
- Falta validar un lote real autorizado.

## 4. Integracion Gold

Piero desarrolla Gold incremental en analytics-piero.

Pendientes:
- Acordar contrato Silver - Gold v0.1.
- Homologar manifests, versiones y claves.
- Emitir eventos solo tras publicacion verificada.
- Probar inserciones y correcciones historicas.
- Comparar Gold incremental y reconstruccion total.

## 5. Migracion Google Cloud

Pendiente:
- Configurar permisos y controles de costos.
- Adaptar almacenamiento a Cloud Storage.
- Desplegar procesamiento Spark en Dataproc.
- Integrar resultados con BigQuery.
- Validar ejecucion distribuida y dashboards.

## 6. Restricciones

No utilizar scripts de laboratorio sobre
Silver oficial.

No publicar registros sinteticos.

No activar Gold sin una version Silver
autorizada y verificada.
