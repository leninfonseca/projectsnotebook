# Real-Time IoT Telemetry Lakehouse — mis apuntes

## Estado actual

Este proyecto está definido en mi portfolio, pero todavía no he implementado el código. Aquí documentaré únicamente código real cuando empiece a construirlo.

## Qué quiero construir

Quiero ingerir telemetría de dispositivos en tiempo casi real y conservar un histórico analizable.

Flujo objetivo:

~~~text
IoT devices
   ↓
Event Hubs / Eventstream
   ↓
OneLake / Delta
   ↓
PySpark Structured Streaming
   ↓
Silver / Gold
   ↓
SQL / Real-Time Analytics
~~~

## Conceptos que tendré que dominar

### Streaming

A diferencia de batch, los datos van llegando continuamente.

### Event time

Es el momento en que ocurrió el evento en el dispositivo.

No debo confundirlo con el momento en que mi plataforma lo recibió.

### Late-arriving events

Un evento puede llegar tarde por red, buffering o desconexiones.

### Windowing

Puedo agrupar eventos en ventanas de tiempo.

Ejemplo conceptual:

~~~text
temperatura media cada 5 minutos
eventos por dispositivo cada 1 minuto
~~~

### Structured Streaming

PySpark me permite expresar transformaciones streaming con una API parecida a DataFrames batch.

## Regla para estas notas

Cuando implemente cada notebook, copiaré el código real y explicaré línea por línea qué hace, qué sintaxis se repite y cómo defenderlo en entrevista.
