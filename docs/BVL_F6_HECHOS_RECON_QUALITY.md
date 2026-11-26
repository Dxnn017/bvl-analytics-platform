# BVL Analytics — Preentrega Quality: Hechos de Importancia V2

**Estado:** `EVIDENCIA_LOCAL_EN_CUARENTENA_NO_AUTORIZA`. **No** habilita ejecución de Silver, consumo de claims ni publicación.
**Base Git que no debe modificarse:** `722e2e131eb8e06d153197c5abe1c2dcc1688896`.

## Resultado real revisado

- 346 identificadores de expedientes históricos: 319 vinculados a PDF Bronze, 27 sin PDF asociados y separados en cuarentena.
- 990 documentos candidatos, con 990 GUID únicos; 989 TXT directos y una vinculación de TXT canónico.
- 990/990 concordancias de SHA y tamaño *registrados* entre el manifiesto de PDF histórico y el manifest Bronze.
- 1,979/1,979 concordancias de rutas HDFS registradas con el manifest Bronze; diferencias del inventario = 0.
- Los 27 eventos en cuarentena no tienen campos `empresa`, `fecha_presentacion`, `fecha_consulta_portal`, `coherencia_temporal`, `usar_silver_principal` acreditados. Se dejan vacíos intencionalmente.
- La comparación histórica registra 27 diferencias en cada uno de esos cinco campos, precisamente por la cuarentena. No es equivalencia semántica total.
- Las relaciones documento ↔ GUID ↔ expediente y otros atributos de portal provienen de auxiliares HDFS históricos **pendientes de ratificación de Quality**. Las claves de los 27 eventos provienen del CSV histórico, no de Bronze.

## Evidencia que **NO** prueba esta preentrega

1. No verifica nuevamente los bytes de cada uno de los 1,979 objetos GCS. La conciliación usa hashes y tamaños declarados en los inventarios oficiales que fueron entregados.
2. No acredita el generador histórico de CSV ni la fuente primaria de las 27 referencias sin PDF.
3. No acredita por sí sola la procedencia primaria del mapeo HDFS GUID ↔ expediente. El procedimiento V2 es reproducible pero usa ese auxiliar y no transforma esa fuente en dato original Bronze.
4. No implica que `hechos_importancia/eventos` esté listo para publicación, ni que los 27 eventos puedan ser cargados en Silver.
5. La validación de estructura de PDF, GUID y hashes de este paquete no constituye aprobación Quality.

## Cómo verificar el paquete sin acceder a nube

Desde el directorio descomprimido:

```bash
python3 VERIFICAR_ENTREGA_HECHOS.py
python3 -m unittest discover -s codigo -p 'test_BVL_F6_*.py' -v
```

Se esperan **23/23 pruebas locales de código** (6 cruce + 9 reconstrucción candidata + 8 cuarentena V2), y los controles de integridad y conteo del verificador offline. Estas pruebas comprueban el código y el contenido presente; el cruce con HDFS/GCS requiere fuentes externas.

## Cómo reproducir desde HDFS y el handoff Bronze oficial

Requisitos en la máquina de validación:
- HDFS de la migración histórica disponible mediante `hdfs dfs`.
- Handoff oficial `entrega_bronze_silver_daniela.tar.gz` (verificarlo con la referencia Quality):
  SHA-256 `ecc5c1d44fcf529c0efab5652dedb488a6149e5f208e4b94ec247847c3898487`.

Desde `codigo`:

```bash
mkdir -p ~/Descargas/BVL_HECHOS_RECON_QA_REPRODUCCION
python3 BVL_F6_HECHOS_RECON_V2.py \
  --handoff ~/bvl-verificacion-quality/entrega_bronze_silver_daniela.tar.gz \
  --out ~/Descargas/BVL_HECHOS_RECON_QA_REPRODUCCION
```

Compare los SHA-256 de los **tres CSV**, que deben coincidir con `resultados/BVL_F6_HECHOS_RECONCILIACION_V2.json` (`output_sha256`). El JSON de reproducción contiene una nueva fecha `generated_at_utc` y no se compara byte por byte con el reporte anterior.

## Qué debe decidir Quality

- Ratificación del origen del auxiliar HDFS GUID ↔ expediente y de sus campos de portal.
- Disposición de los 27 eventos sin PDF: acreditar su fuente original, reconstruirla desde una fuente autorizada o mantenerlos excluidos/cuarentenados.
- Aprobación del nuevo procedimiento de generación, de las reglas de extracción, integridad referencial y diferencias.
- Emisión posterior y separada de autorización específica antes de cualquier ejecución cloud. **No reutilizar** los intentos del piloto ni del release de otros diez procesos.

## Cómo versionar sin modificar el release consolidado

Crear **rama independiente** desde la punta actual `722e2e1`, no sobrescribir el commit ni incluir los CSV sin la autorización de Quality. Los módulos de `codigo/` y este documento pueden incorporarse a un nuevo commit candidato para revisión. Conservar los CSV y evidencias en un paquete local/handoff privado a Quality.

## Inventario

- `codigo/`: generador V1, reconcilador V2, lector/cruce oficial y sus 3 suites de pruebas.
- `resultados/`: 990 documentos, 346 eventos auditables, 27 eventos en cuarentena, JSON de reconciliación.
- `BVL_F6_HECHOS_CRUCE_BRONZE.json` y `BVL_F6_HECHOS_TRAZABILIDAD_DETALLADA.json`: evidencia previa.
- `VERIFICAR_ENTREGA_HECHOS.py`: validación autónoma de salida con checks de integridad/semántica.
- `MANIFEST_SHA256.json`: hashes de todos los archivos de la preentrega.

No hay escrituras Cloud, consumos de claim ni modificaciones de Git causadas por este paquete.
