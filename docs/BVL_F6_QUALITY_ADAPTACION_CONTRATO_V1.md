# BVL F6 — Adaptación candidato Quality, NO AUTORIZA

## Alcance

Se proponen dos adaptaciones independientes en rama aislada desde `8381d36`.

### Montos SAB (INC-003)

- Manifest Bronze completo: **1221 archivos / 22156 registros**.
- Elegibles normales: **1207 archivos / 21911 registros** (candidatos, no publicación completa).
- Cuarentena: **14 archivos / 245 registros**, error `-2147024864` con mensaje exacto acreditado.
- **Cobertura parcial por registros: 21911/22156 (~98.8942%)**. La cobertura **no es 100%**.
- No eliminar las 245 filas, renombrarlas como válidas ni publicar un Gold completo.
- El artefacto de cuarentena debe guardar filas, URI, generación, tamaño, SHA-256, fecha nominal, código y mensaje de error. Se preservan los objetos Bronze originales.
- Nueva autorización **adicional**: `BVL-F6-SMV-MONTOS-PARTIAL-SCOPE-CANDIDATE-v1`, firmada por Quality y vinculada al mismo `run_id`, `attempt_id`, fingerprint y contexto de lanzamiento de Fase 5. Sin esta segunda firma, el launcher falla **antes del claim**. No modificar autorización base ni Fase 5.
- El candidato de publicación final sigue con `quality_approved=false`, `official_publication_allowed=false`, `gold_consumption_allowed=false`.
- Si se encuentra un objeto no clasificado, una huella distinta, un conteo inesperado o cualquier estado distinto del firmado: fallar; **no reintentar** el claim consumido.

### Hechos documental restringido

- Fuente única: manifest oficial fijado de `smv/hechos_importancia` + lectura por **generación/sha256/tamaño exactos** de 990 PDF + 989 TXT en GCS.
- 990 filas documentales, 1 PDF duplicado marcado, 989 TXT distintos conciliados, sin inventar texto.
- GUID inferible del **nombre del archivo**; fecha nominal **no es fecha oficial del evento**.
- No incluir `empresa`, `expediente`, `fecha_presentacion`, `fecha_acuerdo`, `tipo_hecho`, sector ni descripción histórica del portal.
- El módulo propuesto es **adaptador de datos y validación**; no es un launcher cloud ni publica Silver. Para ejecutar en Cloud hacen falta nueva ratificación de mapeo, puerta/contrato, autorizaciones por intento, versión y escritor oficial separado.

### Dependencias Silver / Gold

- `hechos_importancia/identidad_empresarial` histórico: **BLOQUEADO**; su dependencia `hechos_importancia/eventos` no puede sustituirse por documentos para inferir empresas.
- Gold `hechos_empresa` y `impacto_hechos_valor`: **BLOQUEADOS** para resultados empresariales basados en expedientes o fechas históricas no acreditadas.
- El puente `referencias_smv/puente_ruc_nemonico` puede proponerse independientemente, sujeto a Quality; no acredita relación documento → empresa.
- Gold independientes (`mercado_valor_diario`, `actividad_sab_diaria`, `empresa_trimestre`) podrán consumir **únicamente** Silver publicado oficialmente, con versiones y esquemas ratificados.
- Manifest Silver→Gold v0.1: **propuesta**, no evento de publicación autorizado. `INSERT/UPDATE/DELETE`, claves anterior/nueva, counts, particiones, versión previa/actual, eliminación explícita. No usar ausencia como borrado.

### Revisión requerida a Quality

1. Ratificar el esquema de firma adicional de alcance parcial; su ubicación GCS y campos.
2. Revisar cómo el launcher aplica la separación **antes de escribir staging**; verificar la señal de alcance parcial y el reporte de cuarentena.
3. Reproducir con 1221 objetos Bronze, esquema Spark y reconciliación local antes de un lanzamiento.
4. Aprobar revisión diferencial del release reproducible, SHA-256 de módulos, 7 módulos protegidos inalterados.
5. Ratificar adaptación del contrato de Hechos y su vía de autorización cloud; solo tras ello considerar una ejecución supervisada.
6. Coordinar con Piero el bloqueo explícito de Gold de Hechos y el contrato de cambios Silver→Gold.

## Limitaciones

No integra ni publica `pipeline-daniela`; no activa `current.json` ni crea `version_registry`; no firma, autoriza ni consume claims. La correspondencia con el release concreto de la rama debe ser verificada en Debian antes de publicar cualquier commit candidato.
