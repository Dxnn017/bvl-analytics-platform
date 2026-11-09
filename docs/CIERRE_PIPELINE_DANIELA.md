# Entrega tecnica - Pipeline Big Data BVL

## Responsable
Rama: pipeline-daniela

## Componentes implementados

- Ingestion incremental de Cotizaciones.
- Procesamiento incremental de Montos SAB.
- Procesamiento incremental BCRP: Tipo de Cambio y EMBIG.
- Procesamiento incremental de cuatro fuentes financieras.
- Principales Cuentas: claves, colisiones y excepciones.
- Hechos de Importancia: eventos, documentos, texto e identidad.
- Preparacion segura de Gold en staging.
- Ejecutor centralizado con preflight y planificacion.

## Validaciones realizadas

### Principales Cuentas
- Total: 5249 registros.
- Prueba: 4223 historicos + 1026 incorporados.
- Cero registros perdidos.
- Cero diferencias con Silver oficial.
- Tres colisiones conservadas (seis registros).
- Idempotencia aprobada.

### Hechos de Importancia
- Eventos: 346.
- Documentos: 990.
- Contenido textual: 990.
- Identidad empresarial: 338.
- Equivalencia staging/Silver: aprobada 4/4.
- Incorporacion simulada: aprobada 4/4.
- Idempotencia: aprobada.
- Correccion controlada: publicacion bloqueada.

### Escalabilidad
- Pruebas de volumen 1x, 10x y 100x.
- Evidencias de tiempo, throughput y compresion.
- Los datos replicados son pruebas tecnicas,
  no nuevos registros reales de la BVL.

## Restricciones y pendientes

1. El ejecutor integrado solo habilita por ahora
   la reconciliacion de Hechos.
2. Las otras fuentes requieren conectar los lotes
   y configurar sus rutas seguras de ejecucion.
3. Los scripts historicos conservan overwrite,
   fechas y conteos fijos; no deben emplearse
   para publicar nuevas ingestas directamente.
4. Falta automatizar la publicacion transaccional,
   recuperacion ante fallos y verificacion posterior.
5. La regeneracion de Hechos se probo con datos
   historicos; nuevas ingestas requieren adaptar
   los generadores a fechas y volumenes variables.
6. Gold analitico debe validarse antes de publicar.
7. Los puntos del docente sobre referencia ingenua
   del modelo y metricas de proceso de negocio
   requieren evidencias adicionales del equipo.

## Contrato de entrega

Los registros dudosos no se eliminan arbitrariamente.
Bronze conserva los originales.
Silver oficial no fue modificado por las pruebas.
Las publicaciones nuevas requieren validacion y
aprobacion antes de pasar a datos oficiales.
