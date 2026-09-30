# E-commerce Orders, Inventory & Logistics Data Platform — mis apuntes

## Objetivo

Este es mi siguiente proyecto avanzado de Data Engineering local y sin Fabric.

Quiero demostrar que sé construir una plataforma completa sin depender de una nube administrada.

## Stack elegido

- Python
- SQL
- PostgreSQL
- dbt
- Parquet
- Airflow
- Docker
- GitHub Actions
- Power BI Desktop o Streamlit para visualización final

## Fuentes que quiero simular

### Pedidos y pagos

Vendrán de una base de datos.

Quiero tener entidades como:

~~~text
orders
order_items
payments
customers
products
~~~

### Inventario

Llegará mediante CSV.

Esto me permitirá practicar ingestión de ficheros y reconciliación entre fuentes.

### Logística

Quiero utilizar una API simulada.

Ejemplo:

~~~text
shipment_id
order_id
carrier
status
estimated_delivery
actual_delivery
updated_at
~~~

## Datos sintéticos reproducibles

No quiero generar datos aleatorios diferentes cada vez sin control.

Quiero fijar seeds o reglas para que pueda reconstruir exactamente los mismos datasets durante pruebas.

## Problemas de Data Engineering que quiero implementar

### Incremental loading

Procesar solamente lo nuevo o modificado.

### Históricos

Conservar cambios relevantes en el tiempo.

### Deduplicación

Detectar eventos o registros que lleguen repetidos.

### Late-arriving data

Aceptar que algunos eventos de logística pueden llegar después de datos más recientes.

### Reconciliation

Comprobar que diferentes sistemas coincidan.

Ejemplo conceptual:

~~~text
total de payments completados
≈
revenue reconocido en orders
~~~

Si no coincide, quiero detectar la diferencia.

### Data quality

Quiero reglas automáticas que fallen cuando los datos no cumplen condiciones críticas.

### Recovery

Quiero poder reejecutar tareas fallidas sin duplicar datos ni corromper el resultado.

### Performance tests

Quiero aumentar el volumen de datos sintéticos y medir cómo se comporta el pipeline.

## Arquitectura conceptual

~~~text
PostgreSQL orders/payments ─┐
Inventory CSV ──────────────┼─→ ingestion → raw/Parquet
Logistics API ──────────────┘
                               ↓
                         transformations
                               ↓
                              dbt
                               ↓
                    analytical PostgreSQL
                               ↓
                     Power BI / Streamlit
~~~

Airflow orquestará las dependencias y Docker hará reproducible el entorno.

GitHub Actions servirá para CI.

## Dashboard pendiente

Todavía tengo que elegir entre:

**Power BI Desktop**

Me permite demostrar una herramienta BI muy reconocida en ofertas de Data Engineering.

**Streamlit**

Me permite entregar una aplicación completamente reproducible desde el repositorio.

La decisión todavía está pendiente.

## Regla para estas notas

Cuando empiece el proyecto, cada script, DAG, modelo dbt y consulta SQL tendrá:

1. código real;
2. explicación línea por línea;
3. qué problema resuelve;
4. sintaxis que se repite;
5. ejemplo pequeño;
6. cómo lo explicaría en entrevista.

No añadiré código ficticio antes de implementarlo.
