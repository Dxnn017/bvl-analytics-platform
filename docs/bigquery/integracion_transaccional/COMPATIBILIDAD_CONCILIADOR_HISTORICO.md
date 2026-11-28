# Compatibilidad historica SQL-Python

Estado: CANDIDATO LOCAL NO AUTORIZADO.

## Interfaces

Cuatro consultas originales por producto.

Dos consultas complementarias por producto.

Total: 18 interfaces de resultados.

## Normalizacion

DATE a texto ISO canonico.

NUMERIC y BIGNUMERIC a texto decimal exacto.

INT64 a entero sin conversion desde BOOL.

FLOAT64 a numero finito.

NULL se conserva.

No se convierten decimales desde FLOAT64.

Se rechazan aliases faltantes y adicionales.

## Conciliacion

Se reutiliza el conciliador historico original.

Se agregan controles de coherencia entre
minimo, maximo y promedio.

Se validan resultados complementarios de
igualdad historica y layout.

Los conteos deben coincidir con la referencia.

No se aceptan diferencias de registros,
claves invalidas ni duplicadas.

El particionamiento y clustering observados
deben coincidir con el contrato.

## Alcance

Los resultados de prueba son sinteticos.

La comprobacion de aliases es estatica.

No se utiliza el cliente BigQuery.

Se admiten mappings Python normalizados.

Los resultados de una fila deben extraerse
como mapping antes de llamar al adaptador.

No se acredita la adaptacion del cliente cloud
ni la autenticidad de sus resultados.

La comparacion decimal conserva precision
sin redondear valores.

La politica de promedios exactos requiere
revisar la escala de agregacion del origen
frente a BIGNUMERIC de BigQuery.

Las fuentes y resultados deben vincularse
operativamente a una misma version acreditada.

Las metricas sinteticas no demuestran igualdad
real de registros ni layout fisico cloud.

La granularidad de particion y las opciones
de tabla requieren metadata adicional.

Todos los indicadores de autorizacion
productiva permanecen desactivados.
