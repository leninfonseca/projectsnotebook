# Customer 360 & Revenue Data Product — mis apuntes

## Estado actual

Este proyecto forma parte de mi portfolio, pero todavía no he implementado su código. No voy a inventar notebooks, SQL o pipelines que todavía no haya ejecutado.

Cuando empiece a construirlo, iré documentando aquí cada bloque de código exactamente igual que en Barcelona Urban Mobility: código real, explicación línea por línea, patrones repetidos y preguntas de entrevista.

## Qué quiero construir

Quiero integrar datos de:

- CRM;
- clientes;
- pedidos;
- facturación;
- suscripciones;
- soporte.

Mi objetivo es obtener una visión Customer 360 y un modelo de revenue consistente.

## Conceptos que tendré que dominar

### CDC y watermarks

Necesito evitar recargar todo el origen continuamente. Quiero identificar qué registros son nuevos o han cambiado.

### SCD Type 2

Quiero conservar historial de cambios de una dimensión.

Ejemplo conceptual:

~~~text
customer_id | city      | valid_from | valid_to   | current
101         | Barcelona | 2026-01-01 | 2026-05-31 | false
101         | Madrid    | 2026-06-01 | null       | true
~~~

No sobreescribo Barcelona y la pierdo. Mantengo ambas versiones.

### Surrogate keys

Quiero usar claves técnicas internas para mis dimensiones, separadas de los IDs del sistema origen.

### Star schema

Quiero separar hechos y dimensiones.

Ejemplo conceptual:

~~~text
fact_sales
    |
    +-- dim_customer
    +-- dim_date
    +-- dim_product
~~~

## Regla para estas notas

Solo añadiré una sección de código cuando realmente haya ejecutado ese código en el proyecto.
