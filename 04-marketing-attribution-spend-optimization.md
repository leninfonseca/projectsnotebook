# Marketing Attribution & Spend Optimization — mis apuntes

## Estado actual

Este proyecto está definido en mi portfolio, pero todavía no he implementado el código. No quiero llenar mis apuntes con código ficticio.

## Qué quiero construir

Quiero integrar:

- campañas;
- inversión publicitaria;
- conversiones;
- CRM;
- ventas/e-commerce.

Después quiero calcular métricas y modelos de atribución para entender la eficiencia de cada canal.

## Conceptos que tendré que dominar

### Grain

Antes de unir tablas debo definir qué representa una fila.

Ejemplo:

~~~text
una fila por campaña y día
una fila por conversión
una fila por touchpoint
~~~

### ROAS

Conceptualmente:

~~~text
ROAS = revenue atribuido / gasto publicitario
~~~

### CAC

Conceptualmente:

~~~text
CAC = coste de adquisición / clientes adquiridos
~~~

### Attribution

Necesito decidir cómo asigno el valor de una conversión a los diferentes canales o touchpoints.

Ejemplos de enfoques:

- first touch;
- last touch;
- linear;
- modelos más avanzados.

## Regla para estas notas

Cuando implemente SQL, PySpark o pipelines reales, documentaré aquí exactamente el código utilizado y no solo la arquitectura.
