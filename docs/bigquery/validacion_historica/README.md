# Validaciones historicas Gold en BigQuery

Estado: CANDIDATO LOCAL NO AUTORIZADO.

Se generan cuatro consultas por cada producto:

- 01_calidad.sql
- 02_particiones.sql
- 03_esquema.sql
- 04_indicadores.sql

Total: doce consultas de solo lectura.

## Orden operativo propuesto

1. Acreditar autorizacion y destino.
2. Validar esquema, particionamiento y clustering.
3. Ejecutar calidad con expected_rows autorizado.
4. Conciliar conteos por particion.
5. Comparar perfiles numericos con Gold oficial.
6. Registrar resultados y referencias de jobs.

Si falla el esquema, no continuar con las
consultas de datos.

## Conteos

El parametro expected_rows debe obtenerse
del manifest Gold oficialmente aprobado.

Los conteos candidatos son:

- Mercado: 83130.
- SAB: 21911.
- Empresa: 5158.

No sustituir la evidencia oficial por estos
valores candidatos.

## Criterios

- Conteo observado igual al autorizado.
- Claves nulas, vacias y duplicadas: cero.
- Columnas obligatorias nulas: cero.
- Particiones invalidas: cero.
- Valores NaN e infinitos: cero.
- Escala monetaria invalida: cero.
- Columnas faltantes o adicionales: cero.
- Diferencias de tipo, orden y nulabilidad: cero.

## Conciliacion

Las consultas de particiones e indicadores
generan perfiles observados.

Deben compararse con evidencias obtenidas
del Gold oficial correspondiente.

Los perfiles por si solos no demuestran
igualdad completa entre origen y destino.

## Limitaciones

La comprobacion actual es estatica.

No se ejecutaron consultas ni dry runs cloud.

No se acreditaron resultados oficiales.

La configuracion efectiva de particionamiento
y clustering requiere validacion independiente.

La precision original no puede reconstruirse
si una carga anterior ya perdio informacion.

La comparacion fila a fila, las tolerancias
FLOAT64 y la igualdad monetaria requieren
evidencia adicional del origen acreditado.

No se concede autorizacion de ejecucion.
