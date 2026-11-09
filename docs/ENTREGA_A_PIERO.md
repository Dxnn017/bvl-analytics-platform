# Contrato técnico de entrega a Piero — Bolsa de Valores del Perú

**Datos reales y capas disponibles:**
- Silver Cotizaciones (83 130 filas reales hasta 2025-12-31), clave `(fecha_cotizacion, valor)`.
- Gold `mercado/mercado_valor_diario`, llave `(fecha, nemonico)`; datos bursátiles con contextos macroeconómicos y `retorno_diario_pct`.
- Gold `empresas/empresa_trimestre`, llave `(ruc, ejercicio, trimestre)`, sujeto a revisión de colisiones.
- Gold `mercado/actividad_sab_diaria`, llave `(fecha, codigo_sab)`.
- Gold `eventos/hechos_empresa`, llave `numero_expediente`.
- Gold `eventos/impacto_hechos_valor`, llave `(numero_expediente, nemonico)`.

**Para arrancar ya:** Piero puede consumir Gold oficial y verificar columnas/conteos mediante `spark-submit scripts/analytics/auditar_gold_para_piero.py`. No esperar a la ingesta incremental. Mantener split temporal train 2021-2024/test 2025, pero agregar una validación temporal interna para elegir umbral; no ajustar umbral sobre test.

**Problemas del modelo que debe resolver:** la referencia mayoritaria obtiene ~77,72 % de exactitud; modelo previo 77,87 % y solo 49/5 175 altas detectadas. Reportar baseline mayoritario, persistencia de estado (alta volatilidad hoy predice alta mañana), PR-AUC, recall/F1 de clase positiva, coste de falsas alarmas y ROC-AUC. Auditoría de leakage para toda variable temporal, ventanas móviles y Hechos de Importancia. Ante posible cambio de definición de volatilidad, declarar target y su fórmula.

**Regla de integración:** la columna `volatilidad_retorno_pct` de `empresa_trimestre` en el código Gold original usa `stddev(precio_cierre)` (incorrecto). `preparar_gold_seguro.py` propone `stddev(retorno_diario_pct)` en staging, todavía sin publicar. Piero debe registrar versión y origen de indicadores en sus notebooks para no mezclar versiones.

**Lo que Daniela debe entregar:** `outputs/quality/contrato_gold_piero.json`, manifest de Silver, auditoría post-publicación, logs y CSV de benchmarks. Matthew debe coordinar lectura del dashboard sobre datos Gold verificados y aprobados.

**Advertencias:** las réplicas 10× y 100× son únicamente para rendimiento y NUNCA se usan en el modelo. No presentar pruebas `staging` como actualización real de producción. Identificar y validar todas las diferencias entre Gold original y Gold de staging antes de conectar el consumo.
