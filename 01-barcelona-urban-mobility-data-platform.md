# Barcelona Urban Mobility Data Platform — mis apuntes

## Para qué me sirve este documento

Este archivo es mi explicación personal del proyecto. No quiero memorizar código sin entenderlo: quiero poder mirar cada celda, saber qué hace, por qué está ahí y explicarla en una entrevista con mis propias palabras.

Mi flujo actual es:

**REST API → Fabric Pipeline → Bronze JSON → PySpark Notebook → Silver Delta Table**

La fuente actual es contextual y me sirve para validar toda la arquitectura. Todavía no es la fuente principal de movilidad.

---

# 1. Leer el JSON Bronze

## Código que ejecuté

~~~python
bronze_path = "Files/bronze/opendata/district_data/district_data.json"

raw_df = (
    spark.read
    .option("multiline", "true")
    .json(bronze_path)
)

raw_df.printSchema()
~~~

## Qué hago aquí

Creo una variable llamada **bronze_path** que contiene la ruta del fichero JSON que guardé en la capa Bronze.

Después uso **spark.read** para pedirle a Spark que lea datos.

La parte:

~~~python
.option("multiline", "true")
~~~

le indica a Spark que el JSON puede estar escrito en varias líneas. Esto es importante porque el fichero no es una colección simple de objetos independientes, sino una respuesta JSON completa de la API con una estructura anidada.

Con:

~~~python
.json(bronze_path)
~~~

le digo que el formato es JSON y le paso la ruta que quiero leer.

El resultado lo guardo en:

~~~python
raw_df
~~~

Ese objeto es un **DataFrame de Spark**.

Un DataFrame se parece conceptualmente a una tabla con filas y columnas, pero Spark lo puede procesar de forma distribuida.

Finalmente:

~~~python
raw_df.printSchema()
~~~

me enseña la estructura detectada por Spark: nombres de campos, tipos y campos anidados.

## Cómo lo explicaría en entrevista

> En Bronze conservo la respuesta raw de la API. En el notebook la leo con Spark usando multiline JSON y reviso el schema antes de transformar nada. Así puedo comprobar la estructura real del origen antes de definir la lógica Silver.

## Ejemplo sencillo

Si mi JSON fuera:

~~~json
{
  "success": true,
  "result": {
    "records": [
      {"id": 1, "district": "Ciutat Vella"}
    ]
  }
}
~~~

Spark detectaría algo parecido a:

~~~text
root
 |-- success: boolean
 |-- result: struct
     |-- records: array
~~~

Eso me avisa de que **records no está directamente al nivel raíz**: está dentro de result.

## Código, palabras y estructuras que se repiten

**Esto es:** Python + PySpark.

**Patrones que debo reconocer:**

- **variable = valor**: guardo algo para reutilizarlo.
- **spark.read**: inicio una lectura con Spark.
- **.option(...)**: configuro cómo debe comportarse una operación.
- **.json(...)**: indico el formato que quiero leer.
- **DataFrame**: estructura tabular de Spark.
- **.printSchema()**: inspecciono tipos y estructura.
- **métodos encadenados con punto**: Spark usa mucho el patrón objeto.metodo().otroMetodo().
- **paréntesis alrededor de varias líneas**: me permiten escribir una cadena de operaciones de forma legible.

---

# 2. Inspeccionar campos concretos del JSON

## Código que ejecuté

~~~python
display(
    raw_df.select(
        "success",
        "result.resource_id",
        "result.records"
    )
)
~~~

## Qué hago aquí

Uso **select** para escoger solamente las partes del DataFrame que quiero mirar.

No quiero mostrar todo el JSON porque sería difícil de leer. Selecciono:

- **success**: confirma si la API respondió correctamente.
- **result.resource_id**: identifica el recurso de CKAN.
- **result.records**: contiene los registros que realmente me interesan.

La notación:

~~~python
"result.records"
~~~

significa que **records está dentro de result**.

Luego uso **display(...)**, una función muy común en notebooks de Fabric, para visualizar el resultado en forma de tabla.

## Cómo lo explicaría en entrevista

> Antes de transformar la respuesta, inspeccioné los campos relevantes del payload para confirmar que estaba leyendo el recurso correcto y localizar el array de registros que necesitaba normalizar.

## Ejemplo sencillo

Si tengo:

~~~text
person
 ├─ name
 └─ address
     └─ city
~~~

puedo seleccionar:

~~~python
df.select("person.address.city")
~~~

El punto también se usa para navegar por estructuras anidadas.

## Código, palabras y estructuras que se repiten

**Esto es:** selección de columnas en PySpark.

**Patrones importantes:**

- **df.select(...)**: selecciono columnas.
- **"padre.hijo"**: accedo a un campo anidado.
- **display(df)**: visualizo un DataFrame en el notebook.
- **select no modifica el DataFrame original**: devuelve otro DataFrame.

---

# 3. Extraer result.records y convertirlo en filas

## Código que ejecuté

~~~python
from pyspark.sql.functions import explode, col

records_df = (
    raw_df
    .select(
        explode(col("result.records")).alias("record")
    )
)

districts_df = records_df.select("record.*")

display(districts_df)
~~~

## Qué hago aquí

Esta es una de las partes más importantes del notebook.

La API me devuelve **result.records como un array**.

Por ejemplo:

~~~text
records = [
    {registro 1},
    {registro 2},
    {registro 3}
]
~~~

Yo necesito convertir ese array en filas independientes.

### Import

~~~python
from pyspark.sql.functions import explode, col
~~~

Importo dos funciones de PySpark.

**col** representa una columna de Spark.

~~~python
col("result.records")
~~~

significa: quiero trabajar con la columna anidada result.records.

**explode** toma un array y genera una fila por cada elemento.

Si tengo:

~~~text
["A", "B", "C"]
~~~

explode lo convierte conceptualmente en:

~~~text
A
B
C
~~~

### Crear records_df

~~~python
explode(col("result.records")).alias("record")
~~~

Primero localizo la columna records.

Después exploto el array.

Luego uso **alias("record")** para ponerle un nombre fácil al resultado.

Por eso records_df tiene una estructura parecida a:

~~~text
record
 ├─ _id
 ├─ Codi_Barri
 ├─ Codi_Districte
 ├─ Nom_Barri
 ├─ Nom_Districte
 └─ ...
~~~

### Aplanar record

~~~python
districts_df = records_df.select("record.*")
~~~

El asterisco significa **todas las propiedades que existen dentro de record**.

Así convierto:

~~~text
record.Nom_Barri
record.Nom_Districte
record.Valor
~~~

en columnas normales:

~~~text
Nom_Barri
Nom_Districte
Valor
~~~

Eso es lo que normalmente llamo **flattening** o aplanado.

## Cómo lo explicaría en entrevista

> El payload de CKAN tenía los datos dentro de un array anidado en result.records. Utilicé explode para convertir cada objeto del array en una fila y después record.* para aplanar sus atributos como columnas normales.

## Ejemplo sencillo de explode

Antes:

~~~text
id | products
1  | [mouse, keyboard]
~~~

Después de explode:

~~~text
id | product
1  | mouse
1  | keyboard
~~~

## Código, palabras y estructuras que se repiten

**Esto es:** transformación estructural con PySpark.

**Debo reconocer:**

- **from ... import ...**: importo funciones que voy a utilizar.
- **col("nombre")**: convierto un nombre de columna en una expresión Spark.
- **explode(...)**: convierto elementos de arrays en filas.
- **alias("nuevo_nombre")**: renombro el resultado de una expresión.
- **record.***: selecciono todos los campos internos.
- **df = otro_df.select(...)**: genero un DataFrame nuevo a partir de otro.
- Spark suele trabajar de forma **inmutable**: no cambio districts_df dentro del original; voy construyendo nuevos DataFrames.

---

# 4. Construir el DataFrame Silver

## Código que ejecuté

~~~python
from pyspark.sql.functions import (
    col,
    current_timestamp,
    to_timestamp,
    trim
)

silver_df = (
    districts_df
    .select(
        col("_id").cast("long").alias("source_id"),
        trim(col("AEB")).alias("aeb"),
        trim(col("Codi_Barri")).alias("neighborhood_code"),
        trim(col("Codi_Districte")).alias("district_code"),
        trim(col("Nom_Barri")).alias("neighborhood_name"),
        trim(col("Nom_Districte")).alias("district_name"),
        trim(col("Seccio_Censal")).alias("census_section"),
        trim(col("NACIONALITAT_DOMICILI")).alias("nationality_code"),
        col("Valor").cast("double").alias("value"),
        to_timestamp(col("Data_Referencia")).alias("reference_date")
    )
    .dropDuplicates()
    .withColumn("silver_processed_at", current_timestamp())
)

display(silver_df)
~~~

## Qué hago aquí

Aquí empiezo realmente a construir **Silver**.

Bronze conserva los nombres y tipos originales del origen.

Silver debe tener una estructura más limpia, consistente y preparada para reutilizarse.

### col(...)

~~~python
col("_id")
~~~

Le digo a Spark que quiero trabajar con la columna _id.

Esto es distinto de escribir solamente una cadena cuando necesito aplicar funciones sobre la columna.

### cast(...)

~~~python
col("_id").cast("long")
~~~

Con **cast** convierto el tipo.

Long es un entero grande.

Para Valor hago:

~~~python
col("Valor").cast("double")
~~~

Double es un número decimal.

No quiero confiar únicamente en los tipos inferidos automáticamente. En una capa curada es mejor controlar explícitamente los tipos importantes.

### alias(...)

~~~python
.alias("source_id")
~~~

Renombro las columnas.

Por ejemplo:

~~~text
Nom_Districte → district_name
Codi_Barri     → neighborhood_code
Valor          → value
~~~

Esto hace que el modelo Silver tenga nombres consistentes y entendibles.

### trim(...)

~~~python
trim(col("Nom_Barri"))
~~~

trim elimina espacios sobrantes al principio y al final.

Ejemplo:

~~~text
"  el Raval  "
~~~

se convierte en:

~~~text
"el Raval"
~~~

Es una limpieza sencilla, pero evita problemas futuros en comparaciones y joins.

### Por qué dejo códigos como string

Aunque district_code o neighborhood_code parezcan números, conceptualmente son identificadores.

No quiero hacer:

~~~text
district_code + neighborhood_code
~~~

porque no tendría significado.

Por eso los mantengo como texto.

### to_timestamp(...)

~~~python
to_timestamp(col("Data_Referencia"))
~~~

Convierto la fecha de origen en un tipo temporal real.

Eso permite después:

- ordenar por fecha;
- filtrar intervalos;
- agrupar por tiempo;
- calcular diferencias;
- usar funciones temporales.

### dropDuplicates()

~~~python
.dropDuplicates()
~~~

Elimina filas completamente duplicadas.

No estoy diciendo que source_id sea automáticamente la clave de deduplicación aquí. En este paso elimino duplicados exactos del DataFrame.

### withColumn(...)

~~~python
.withColumn("silver_processed_at", current_timestamp())
~~~

withColumn crea o reemplaza una columna.

Yo creo silver_processed_at con la hora actual de procesamiento.

Esto es **metadata técnica**.

No describe cuándo ocurrió el dato de negocio, sino cuándo lo procesé en Silver.

## Cómo lo explicaría en entrevista

> En Silver normalicé nombres, convertí los tipos explícitamente, limpié strings, eliminé duplicados exactos y añadí metadata de procesamiento. Mantuve los códigos geográficos como strings porque son identificadores, no medidas.

## Diferencia importante

**reference_date** = fecha que viene del origen.

**silver_processed_at** = momento en que mi pipeline procesó el dato.

No son lo mismo.

## Código, palabras y estructuras que se repiten

**Esto es:** limpieza y estandarización de un DataFrame.

**Patrones esenciales:**

- **col(...)**: referencia a una columna.
- **cast(...)**: conversión de tipo.
- **alias(...)**: cambio de nombre.
- **trim(...)**: limpieza de espacios.
- **to_timestamp(...)**: conversión temporal.
- **dropDuplicates()**: eliminación de duplicados.
- **withColumn(nombre, expresión)**: creo o modifico una columna.
- **current_timestamp()**: fecha/hora actual del procesamiento.
- **método tras método**: select → dropDuplicates → withColumn.
- Esta cadena representa una **secuencia de transformaciones**.

---

# 5. Comparar filas y revisar nulos

## Código que ejecuté

~~~python
from pyspark.sql.functions import count, when

print(f"Bronze records: {districts_df.count()}")
print(f"Silver records: {silver_df.count()}")

display(
    silver_df.select([
        count(
            when(col(c).isNull(), c)
        ).alias(c)
        for c in silver_df.columns
    ])
)
~~~

## Qué hago aquí

Quiero comprobar que la transformación no haya eliminado registros inesperadamente y revisar los nulos de todas las columnas.

### count() como acción

~~~python
districts_df.count()
~~~

cuenta las filas.

En este caso obtuve:

~~~text
Bronze records: 100
Silver records: 100
~~~

Eso significa que mi transformación mantuvo las 100 filas de la muestra.

### f-string

~~~python
f"Bronze records: {districts_df.count()}"
~~~

La letra f permite insertar valores dentro de una cadena usando llaves.

Ejemplo:

~~~python
name = "Barcelona"
print(f"City: {name}")
~~~

resultado:

~~~text
City: Barcelona
~~~

### Recorrer todas las columnas

~~~python
for c in silver_df.columns
~~~

silver_df.columns devuelve una lista con los nombres de las columnas.

El for va pasando una columna cada vez por la variable c.

### isNull()

~~~python
col(c).isNull()
~~~

pregunta si el valor de esa columna es nulo.

### when(...)

~~~python
when(col(c).isNull(), c)
~~~

Es parecido a un IF.

Conceptualmente:

~~~text
SI la columna es NULL
ENTONCES devuelve algo
~~~

### count(when(...))

Al envolverlo en count cuento cuántas filas cumplen la condición de nulo.

Finalmente uso alias(c) para que el resultado tenga el mismo nombre de la columna que estoy comprobando.

## La estructura más rara: list comprehension

~~~python
[
    expresion
    for c in silver_df.columns
]
~~~

Esto es Python.

Construye una lista automáticamente.

Una versión más larga conceptualmente sería:

~~~python
results = []

for c in silver_df.columns:
    results.append(expresion)
~~~

La versión compacta hace lo mismo de forma más idiomática.

## Cómo lo explicaría en entrevista

> Comparé el número de registros antes y después de la transformación y generé una comprobación de nulos para todas las columnas. Así pude detectar pérdidas de datos o problemas de completitud antes de persistir Silver.

## Código, palabras y estructuras que se repiten

**Esto mezcla:** Python + expresiones PySpark.

**Debo reconocer:**

- **print(...)**: salida de texto.
- **f"..."**: f-string de Python.
- **df.count()**: cuenta filas.
- **df.columns**: lista de nombres de columnas.
- **for ... in ...**: iteración.
- **[expresión for x in lista]**: list comprehension.
- **isNull()**: condición de nulo.
- **when(condición, valor)**: IF de PySpark.
- **count(expresión)**: agregación.

Importante: hay dos usos distintos de count.

~~~python
df.count()
~~~

cuenta filas del DataFrame.

~~~python
count(columna)
~~~

es una función de agregación dentro de una expresión Spark.

---

# 6. Escribir la tabla Delta Silver

## Código que ejecuté

~~~python
silver_table = "silver_district_context"

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

print(f"Delta table created: {silver_table}")
~~~

## Qué hago aquí

Hasta este punto silver_df existe como DataFrame dentro de mi sesión de Spark.

Ahora quiero **persistirlo físicamente como una tabla Delta** en el Lakehouse.

### silver_df.write

~~~python
silver_df.write
~~~

Inicio el proceso de escritura.

### format("delta")

~~~python
.format("delta")
~~~

Indico que quiero guardar en formato Delta Lake.

Delta no es simplemente un CSV o Parquet suelto. Añade una capa transaccional y metadata que permite operaciones más robustas.

### mode("overwrite")

~~~python
.mode("overwrite")
~~~

Indica qué debe ocurrir si la tabla ya existe.

Overwrite significa reemplazarla.

Lo uso ahora porque estoy haciendo la carga inicial y estoy desarrollando el pipeline.

Más adelante, para incrementalidad, no quiero sobrescribir todo cada vez. Ahí utilizaré estrategias como MERGE/upsert.

### overwriteSchema

~~~python
.option("overwriteSchema", "true")
~~~

Si el schema cambia durante esta fase de desarrollo, permito reemplazar el schema existente.

No significa que en producción quiera aceptar cualquier cambio sin control.

### saveAsTable

~~~python
.saveAsTable(silver_table)
~~~

Persiste el DataFrame como una tabla registrada en el Lakehouse.

El nombre final es:

~~~text
silver_district_context
~~~

## Cómo lo explicaría en entrevista

> Una vez validado el DataFrame, lo persistí como una Delta table del Lakehouse. Para la carga inicial utilicé overwrite; la incrementalidad y MERGE los añadiré en una fase posterior.

## Código, palabras y estructuras que se repiten

**Esto es:** escritura de datos con Spark.

**Patrón típico:**

~~~text
df.write
  → format
  → mode
  → options
  → save
~~~

**Palabras clave:**

- **write**: inicio escritura.
- **format**: formato físico/lógico.
- **delta**: Delta Lake.
- **mode**: comportamiento de escritura.
- **overwrite**: reemplazar.
- **option**: configuración adicional.
- **saveAsTable**: guardar y registrar como tabla.

---

# 7. Leer la tabla Silver otra vez

## Código que ejecuté

~~~python
silver_table_df = spark.table("silver_district_context")

print(f"Rows in Silver Delta table: {silver_table_df.count()}")

silver_table_df.printSchema()

display(silver_table_df)
~~~

## Qué hago aquí

No me conformo con que la escritura no dé error.

Vuelvo a leer la tabla desde el catálogo usando:

~~~python
spark.table("silver_district_context")
~~~

Así valido que realmente quedó registrada y accesible.

Después:

- cuento las filas;
- reviso el schema;
- visualizo los datos.

El resultado fue:

~~~text
Rows in Silver Delta table: 100
~~~

## Cómo lo explicaría en entrevista

> Después de escribir la tabla, la volví a leer desde el catálogo para validar persistencia, row count y schema. No consideré completa la transformación solo porque el write hubiera terminado sin excepción.

## Código, palabras y estructuras que se repiten

**Esto es:** lectura y validación de una tabla ya persistida.

- **spark.table(nombre)**: leo una tabla registrada.
- **count()**: compruebo filas.
- **printSchema()**: compruebo tipos.
- **display()**: inspecciono datos.

Es un patrón que volveré a usar mucho:

~~~text
escribo → vuelvo a leer → valido
~~~

---

# 8. Controles de calidad adicionales

## Código que ejecuté

~~~python
from pyspark.sql.functions import col, count, countDistinct, min, max

total_rows = silver_df.count()
unique_ids = silver_df.select("source_id").distinct().count()
duplicate_ids = total_rows - unique_ids

null_critical = silver_df.filter(
    col("source_id").isNull()
    | col("district_code").isNull()
    | col("district_name").isNull()
    | col("value").isNull()
).count()

negative_values = silver_df.filter(col("value") < 0).count()

print(f"Total rows: {total_rows}")
print(f"Unique source_id: {unique_ids}")
print(f"Duplicate source_id: {duplicate_ids}")
print(f"Rows with nulls in critical columns: {null_critical}")
print(f"Rows with negative values: {negative_values}")

display(
    silver_df.select(
        min("value").alias("min_value"),
        max("value").alias("max_value"),
        countDistinct("district_code").alias("district_count"),
        countDistinct("neighborhood_code").alias("neighborhood_count")
    )
)
~~~

## Qué hago aquí

Esta celda convierte mis comprobaciones en controles de calidad más concretos.

### total_rows

~~~python
total_rows = silver_df.count()
~~~

Número total de filas.

### unique_ids

~~~python
silver_df.select("source_id").distinct().count()
~~~

Primero selecciono solo source_id.

Después distinct elimina valores repetidos.

Después count cuenta cuántos identificadores diferentes hay.

### duplicate_ids

~~~python
duplicate_ids = total_rows - unique_ids
~~~

Si tengo 100 filas y 100 IDs únicos:

~~~text
100 - 100 = 0 duplicados
~~~

### filter(...)

~~~python
silver_df.filter(condicion)
~~~

Me quedo solo con las filas que cumplen una condición.

### Operador |

~~~python
condicion1 | condicion2
~~~

En expresiones PySpark, la barra vertical funciona como OR lógico.

Quiero detectar una fila si **cualquiera** de las columnas críticas es nula.

~~~python
col("source_id").isNull()
| col("district_code").isNull()
| col("district_name").isNull()
| col("value").isNull()
~~~

### Valores negativos

~~~python
silver_df.filter(col("value") < 0).count()
~~~

Me quedo con las filas donde value es menor que cero y las cuento.

La lógica de negocio actual espera que no existan valores negativos.

### min y max

~~~python
min("value")
max("value")
~~~

Me ayudan a revisar el rango de los datos.

### countDistinct

~~~python
countDistinct("district_code")
~~~

cuenta cuántos valores diferentes existen sin tener que hacer select().distinct().count() manualmente.

## Cómo lo explicaría en entrevista

> Añadí checks de unicidad, completitud y rango. También calculé métricas exploratorias como mínimo, máximo y cardinalidad de distritos y barrios para validar que el dataset tuviera una forma razonable.

## Código, palabras y estructuras que se repiten

**Esto es:** data quality + agregaciones PySpark.

- **distinct()**: elimina valores repetidos.
- **filter(condición)**: conserva filas que cumplen una condición.
- **|**: OR lógico en expresiones Spark.
- **<, >, ==**: comparaciones.
- **min / max**: agregaciones.
- **countDistinct**: cardinalidad.
- **alias**: nombre de una métrica.
- **variable = cálculo**: guardo resultados para reutilizarlos.

Una idea que debo aprender:

~~~text
filter + count = cuántas filas incumplen una regla
~~~

Ejemplo:

~~~python
invalid = df.filter(col("age") < 0).count()
~~~

---

# 9. Hacer que el notebook falle si la calidad no es válida

## Código que ejecuté

~~~python
assert total_rows > 0, "Silver table is empty"
assert duplicate_ids == 0, "Duplicate source_id values detected"
assert null_critical == 0, "Null values detected in critical columns"
assert negative_values == 0, "Negative values detected in value column"

print("All Silver data quality checks passed.")
~~~

## Qué hago aquí

Esta parte es pequeña pero muy importante.

Un check que solo imprime:

~~~text
hay 25 duplicados
~~~

puede pasar desapercibido.

Con **assert**, hago que el código falle si una condición no se cumple.

Ejemplo:

~~~python
assert duplicate_ids == 0
~~~

significa:

~~~text
Exijo que duplicate_ids sea exactamente 0.
Si no lo es, detengo la ejecución con error.
~~~

El segundo argumento es el mensaje que quiero ver si falla.

~~~python
assert total_rows > 0, "Silver table is empty"
~~~

Si total_rows vale 0, el notebook lanza un AssertionError con ese texto.

En mi ejecución, todas las reglas pasaron:

~~~text
All Silver data quality checks passed.
~~~

## Cómo lo explicaría en entrevista

> No dejé los controles solo como métricas informativas. Convertí las reglas críticas en assertions para que el notebook falle si Silver está vacío, existen IDs duplicados, faltan campos críticos o aparecen valores negativos.

## Código, palabras y estructuras que se repiten

**Esto es:** Python puro usado para validar resultados calculados con Spark.

- **assert condición, "mensaje"**: exige que una condición sea verdadera.
- **==**: igualdad.
- **>**: mayor que.
- **0**: muchas reglas de calidad expresan que el número de errores debe ser cero.
- **fail fast**: prefiero detener el pipeline a propagar datos incorrectos.

Patrón que debo recordar:

~~~python
invalid_rows = ...
assert invalid_rows == 0, "Quality rule failed"
~~~

---

# 10. Qué significa realmente Bronze → Silver

No debo pensar que Silver significa solamente cambiar nombres.

Mi transformación actual hace varias cosas:

~~~text
RAW JSON
   ↓
leer estructura
   ↓
localizar result.records
   ↓
explode
   ↓
flatten
   ↓
normalizar nombres
   ↓
convertir tipos
   ↓
limpiar strings
   ↓
eliminar duplicados
   ↓
añadir metadata
   ↓
validar calidad
   ↓
guardar como Delta
~~~

Eso es una transformación Bronze → Silver real, aunque el dataset todavía sea pequeño.

---

# 11. Conceptos que debo dominar porque aparecen continuamente

## DataFrame

Es la estructura principal con la que trabajo en Spark.

Ejemplos de mis DataFrames:

~~~text
raw_df
records_df
districts_df
silver_df
silver_table_df
~~~

Los nombres me ayudan a entender en qué etapa estoy.

## Transformación vs acción

Spark utiliza evaluación lazy.

Muchas instrucciones describen lo que quiero hacer pero Spark no ejecuta necesariamente todo en ese instante.

Transformaciones típicas:

~~~text
select
filter
withColumn
dropDuplicates
~~~

Acciones que obligan a Spark a producir un resultado:

~~~text
count
display
write/save
~~~

Esta distinción es importante para entrevistas de Spark.

## Method chaining

Ejemplo:

~~~python
df.select(...)
  .dropDuplicates()
  .withColumn(...)
~~~

Cada método devuelve otro DataFrame y sigo encadenando operaciones.

## col

Lo uso cuando quiero construir una expresión sobre una columna.

~~~python
col("value") < 0
col("name").isNull()
col("_id").cast("long")
~~~

## alias

Le pongo un nombre al resultado de una expresión.

~~~python
col("Valor").cast("double").alias("value")
~~~

## select

Escoge columnas o expresiones.

## filter

Escoge filas.

Esta diferencia debo tenerla clarísima:

~~~text
select → columnas
filter → filas
~~~

## cast

Convierte tipos.

## withColumn

Crea o modifica una columna.

## count

Cuenta.

Pero debo mirar el contexto porque puede ser:

~~~python
df.count()
~~~

o una función de agregación:

~~~python
count(col("x"))
~~~

## distinct y dropDuplicates

**distinct()** devuelve filas/valores únicos del conjunto seleccionado.

**dropDuplicates()** elimina filas duplicadas del DataFrame y también puede usarse con columnas concretas si se especifican.

---

# 12. Preguntas de entrevista que ya debería poder responder

## ¿Por qué Bronze almacena el JSON raw?

Porque quiero preservar el payload original. Si la transformación falla o cambia una regla de negocio, puedo reprocesar desde el dato original sin volver a depender de la API.

## ¿Por qué utilizaste explode?

Porque result.records era un array. Necesitaba transformar cada objeto del array en una fila para poder trabajar con un DataFrame tabular.

## ¿Qué diferencia hay entre Bronze y Silver?

Bronze conserva el origen prácticamente sin transformar. Silver contiene datos estructurados, tipados, normalizados y validados para reutilización.

## ¿Por qué Delta?

Porque quiero una capa curada con capacidades transaccionales, schema management y soporte para patrones posteriores como MERGE/upsert.

## ¿Por qué overwrite?

Porque esta es mi carga inicial durante el desarrollo. No es la estrategia incremental definitiva.

## ¿Por qué district_code es string?

Porque es un identificador. Aunque tenga dígitos, no representa una cantidad matemática.

## ¿Qué haces para evitar propagar datos incorrectos?

Calculo métricas de calidad y utilizo assertions para detener el notebook cuando falla una regla crítica.

## ¿Por qué guardas silver_processed_at?

Para tener trazabilidad de cuándo pasó cada registro por mi transformación Silver.

---

# 13. Lo que todavía NO he implementado

No debo decir en una entrevista que ya hice algo que todavía no existe.

Ya implementé después de esta primera fase:

- fuente Bicing orientada a movilidad;
- snapshots históricos Bronze;
- incremental loading;
- watermark;
- Delta MERGE idempotente;
- orquestación Copy → Notebook.

Todavía me falta:

- Silver enrichment entre distintas fuentes;
- Gold;
- modelo dimensional;
- SQL analítico;
- visualización final.

Puedo explicar que forman parte de la arquitectura objetivo, pero debo separar claramente **implementado** de **planificado**.

---

# 14. Resumen de 30 segundos

> Construí una ingesta en Microsoft Fabric desde la API CKAN de Open Data Barcelona hacia una capa Bronze en OneLake. Después utilicé un notebook PySpark para leer el JSON anidado, extraer y explotar result.records, normalizar nombres y tipos, añadir metadata y ejecutar controles automáticos de calidad. Finalmente persistí el resultado como una Delta table Silver y la volví a leer para validar schema y row count.

Ese resumen no sustituye entender el código. El objetivo de estas notas es que pueda explicar qué ocurre debajo de cada frase.


---

# 15. Bicing histórico e incremental

Después de la primera Silver de Bicing, dejé de guardar un único `bicing_snapshot.json` que se sobrescribía.

Ahora cada ejecución del pipeline crea un snapshot independiente:

~~~text
Files/bronze/citybikes/bicing/history/
└── year=2026/
    └── month=10/
        └── day=03/
            ├── bicing_20261003_134326.json
            └── bicing_20261003_134432.json
~~~

Esto me permite conservar cómo estaba la red de Bicing en distintos momentos.

## Expresión de Fabric para la carpeta

~~~text
@concat(
    'bronze/citybikes/bicing/history/year=',
    formatDateTime(utcNow(),'yyyy'),
    '/month=',
    formatDateTime(utcNow(),'MM'),
    '/day=',
    formatDateTime(utcNow(),'dd')
)
~~~

Esto no es Python ni SQL. Es el lenguaje de expresiones de Fabric Data Factory.

- `@` indica que Fabric debe evaluar una expresión.
- `concat()` une textos.
- `utcNow()` devuelve la fecha/hora actual UTC.
- `formatDateTime()` da formato a una fecha.

El nombre del archivo se genera con:

~~~text
@concat(
    'bicing_',
    formatDateTime(utcNow(),'yyyyMMdd_HHmmss'),
    '.json'
)
~~~

Así cada ejecución crea un archivo distinto.

## Cómo lo explicaría en entrevista

> Al principio sobrescribía un único snapshot. Cambié el destino del pipeline para generar snapshots timestamped y particionados por año, mes y día. De esta forma Bronze conserva el histórico y puede utilizarse para replay e incremental processing.

---

# 16. Qué es incremental load

Una carga incremental significa que no vuelvo a procesar todo el histórico cada vez.

Sin incrementalidad:

~~~text
1000 snapshots históricos
+ 1 snapshot nuevo
↓
volver a leer 1001 snapshots
~~~

Con incrementalidad:

~~~text
último snapshot procesado
↓
detecto los posteriores
↓
leo únicamente lo nuevo
~~~

Eso reduce I/O, transformaciones Spark y coste de cómputo.

---

# 17. Obtener el watermark

## Código

~~~python
table_exists = spark.catalog.tableExists(
    HISTORY_TABLE
)

if table_exists:
    watermark = (
        spark.table(HISTORY_TABLE)
        .agg(
            spark_max(
                "snapshot_ingested_at"
            ).alias("watermark")
        )
        .first()["watermark"]
    )
else:
    watermark = None
~~~

## Qué significa watermark

El watermark es la referencia que me dice:

> Hasta este momento ya procesé los datos.

En mi proyecto uso:

~~~text
MAX(snapshot_ingested_at)
~~~

Si el máximo es:

~~~text
2026-10-03 14:34:00
~~~

solo me interesan snapshots posteriores.

## Partes importantes

~~~python
spark.catalog.tableExists(HISTORY_TABLE)
~~~

Comprueba si la tabla existe.

Esto permite que el mismo notebook funcione tanto para:

~~~text
bootstrap
→ primera carga

incremental
→ cargas posteriores
~~~

~~~python
.agg(spark_max("snapshot_ingested_at"))
~~~

Hace una agregación para obtener el timestamp máximo.

~~~python
.first()["watermark"]
~~~

Toma la primera fila del resultado y accede al campo llamado watermark.

## Patrón reutilizable

~~~text
¿Existe target?
↓
NO → watermark = None → initial load
SÍ → watermark = MAX(fecha_procesada) → incremental
~~~

---

# 18. Listar Bronze sin leer todos los JSON

## Código

~~~python
def list_files_recursive(path):
    files = []

    for item in notebookutils.fs.ls(path):
        if item.isDir:
            files.extend(
                list_files_recursive(item.path)
            )
        else:
            files.append(item.path)

    return files
~~~

## Qué hago

`notebookutils.fs.ls()` lista elementos del filesystem de Fabric.

Mi Bronze está dentro de varias carpetas:

~~~text
history/year/month/day/files
~~~

Por eso necesito una función recursiva.

Una función recursiva es una función que puede llamarse a sí misma.

Si encuentra una carpeta:

~~~python
list_files_recursive(item.path)
~~~

entra dentro de esa carpeta.

Si encuentra un archivo:

~~~python
files.append(item.path)
~~~

lo añade a la lista.

## Idea importante

Aquí todavía no estoy leyendo el contenido JSON.

Estoy mirando metadata de archivos para decidir qué necesito procesar.

Eso es mucho más barato que abrir y transformar todos los snapshots.

---

# 19. Detectar snapshots posteriores al watermark

## Código

~~~python
snapshot_pattern = re.compile(
    r"bicing_(\d{8}_\d{6})\.json$"
)

new_snapshot_files = []

for file_path in all_snapshot_files:

    match = snapshot_pattern.search(
        file_path
    )

    if match:

        snapshot_datetime = datetime.strptime(
            match.group(1),
            "%Y%m%d_%H%M%S"
        )

        if (
            watermark is None
            or snapshot_datetime > watermark
        ):
            new_snapshot_files.append(
                file_path
            )
~~~

## Regex

~~~text
bicing_(\d{8}_\d{6})\.json$
~~~

Busca nombres como:

~~~text
bicing_20261003_143400.json
~~~

y captura:

~~~text
20261003_143400
~~~

### \d{8}

Ocho dígitos:

~~~text
20261003
~~~

### _

Guion bajo literal.

### \d{6}

Seis dígitos:

~~~text
143400
~~~

### $

Indica final del texto.

## datetime.strptime

~~~python
datetime.strptime(
    "20261003_143400",
    "%Y%m%d_%H%M%S"
)
~~~

convierte texto en un datetime que Python puede comparar.

## Condición incremental

~~~python
if (
    watermark is None
    or snapshot_datetime > watermark
):
~~~

Significa:

~~~text
si todavía no hay watermark
→ procesar

O

si snapshot > watermark
→ procesar
~~~

---

# 20. El caso de 0 datos nuevos

## Código

~~~python
if len(new_snapshot_files) == 0:
    print(
        "No new snapshots to process. "
        "Silver history is already up to date."
    )

    notebookutils.notebook.exit(
        "No new snapshots to process."
    )
~~~

## Por qué es importante

No recibir nuevos datos no es un error.

Un pipeline bien diseñado debe poder hacer:

~~~text
0 datos nuevos
↓
terminar correctamente
~~~

y no:

~~~text
0 datos
↓
spark.read.json([])
↓
error
~~~

Esto también evita iniciar transformaciones innecesarias.

---

# 21. Leer solamente los archivos nuevos

## Código

~~~python
incremental_raw_df = (
    spark.read
    .option("multiline", "true")
    .json(new_snapshot_files)
    .withColumn(
        "source_file",
        input_file_name()
    )
)
~~~

La diferencia clave es:

~~~python
.json(new_snapshot_files)
~~~

No leo `HISTORY_ROOT` completo.

Leo solo la lista filtrada.

Eso es lo que convierte el proceso en incremental de verdad.

~~~python
input_file_name()
~~~

añade la ruta física del archivo del que procede cada registro.

La utilizo después para obtener el timestamp del snapshot.

---

# 22. Convertir network.stations en observaciones históricas

## Código

~~~python
incremental_stations_df = (
    incremental_raw_df
    .select(
        "source_file",
        explode(
            col("network.stations")
        ).alias("station")
    )
    .withColumn(
        "snapshot_text",
        regexp_extract(
            col("source_file"),
            r"bicing_(\d{8}_\d{6})\.json$",
            1
        )
    )
    .withColumn(
        "snapshot_ingested_at",
        to_timestamp(
            col("snapshot_text"),
            "yyyyMMdd_HHmmss"
        )
    )
    .select(
        "source_file",
        "snapshot_ingested_at",
        "station.*"
    )
)
~~~

## Lo importante

`explode()` convierte cada estación del array en una fila.

`regexp_extract()` obtiene la fecha/hora del nombre del archivo.

`to_timestamp()` la convierte en timestamp real.

Así cada observación conoce cuándo la capturé.

---

# 23. Los tres tiempos del modelo

Ahora conservo tres tiempos diferentes.

## source_timestamp

~~~text
¿Cuándo dice CityBikes que se actualizó esa estación?
~~~

## snapshot_ingested_at

~~~text
¿Cuándo capturó mi pipeline ese estado?
~~~

## silver_processed_at

~~~text
¿Cuándo transformó Spark ese registro?
~~~

No debo confundirlos.

La clave histórica correcta es:

~~~text
station_id + snapshot_ingested_at
~~~

porque puedo capturar dos veces una estación aunque CityBikes mantenga el mismo source_timestamp.

---

# 24. Data Quality incremental

Antes del MERGE calculo:

~~~text
incremental_rows
incremental_duplicates
incremental_null_critical
incremental_null_availability
incremental_invalid_availability
incremental_invalid_coordinates
incremental_warnings
~~~

Después convierto las reglas críticas en `assert`.

Ejemplo:

~~~python
assert incremental_duplicates == 0, \
    "Duplicate incremental observations detected"
~~~

El bike breakdown sigue siendo warning:

~~~python
if incremental_warnings > 0:
    print(
        f"WARNING: {incremental_warnings} incremental records "
        "have an inconsistent bike breakdown."
    )
~~~

## Regla mental

~~~text
ERROR crítico
→ no confío en Silver
→ detener

WARNING
→ dato puede seguir siendo útil
→ conservar + marcar
~~~

---

# 25. Delta MERGE

## Código conceptual

~~~python
target_delta.alias("target").merge(
    incremental_silver_df.alias("source"),
    """
    target.station_id = source.station_id
    AND target.snapshot_ingested_at =
        source.snapshot_ingested_at
    """
).whenNotMatchedInsertAll().execute()
~~~

## Qué es target

La tabla Silver que ya existe.

## Qué es source

El nuevo batch que quiero cargar.

## Condición

~~~text
mismo station_id
Y
mismo snapshot_ingested_at
~~~

significa que es la misma observación histórica.

## whenNotMatchedInsertAll

Si no existe en target:

~~~text
INSERT
~~~

Si ya existe, no hago nada.

No hago update porque quiero que los snapshots históricos sean inmutables.

---

# 26. Idempotencia

Idempotencia significa:

> Ejecutar el mismo proceso varias veces deja el mismo resultado final.

Prueba que hice:

~~~text
Rows before MERGE: 1088
Rows after MERGE: 1088
Rows inserted: 0
~~~

Eso demuestra que volver a procesar el mismo batch no duplica filas.

Después, cuando llegó un snapshot nuevo:

~~~text
Rows before MERGE: 1088
Rows after MERGE: 1631
Rows inserted: 543
~~~

Solo se insertó lo que no existía.

## Cómo lo explicaría en entrevista

> I use a Delta MERGE keyed by station_id and snapshot_ingested_at. Reprocessing the same Bronze snapshot does not create duplicate Silver observations, so the load is idempotent.

---

# 27. Por qué un snapshot tuvo 543 y otros 544 estaciones

No debo asumir que la API siempre devuelve exactamente el mismo número de estaciones.

Comprobé con un left anti join qué estación estaba presente en el snapshot anterior pero no en el nuevo.

~~~python
previous_stations.join(
    latest_stations,
    on="station_id",
    how="left_anti"
)
~~~

## Qué hace left_anti

~~~text
dame filas de A
que NO tienen coincidencia en B
~~~

Lo utilicé como diagnóstico temporal, no como parte del pipeline final.

La enseñanza importante es:

~~~text
No codificar reglas de calidad basadas en suposiciones no garantizadas por la fuente.
~~~

---

# 28. Validación final y avance del watermark

Después del MERGE vuelvo a leer la tabla.

Compruebo:

- número total de filas;
- número de snapshots;
- duplicados;
- watermark anterior;
- watermark nuevo.

También valido:

~~~python
if watermark is not None and rows_inserted > 0:
    assert new_watermark > watermark
~~~

Si inserté nuevos snapshots, el watermark debe avanzar.

Este patrón sirve para comprobar que el incremental load realmente progresó.

---

# 29. Orquestación end-to-end

Finalmente conecté el notebook al pipeline:

~~~text
cp_ingest_bicing_bronze
        │
        │ Success
        ▼
nb_process_bicing_history
~~~

Eso significa que ya no necesito:

~~~text
ejecutar Copy manualmente
↓
ir al notebook
↓
ejecutarlo manualmente
~~~

Ahora una ejecución del pipeline hace:

~~~text
API
↓
Bronze histórico
↓
Notebook incremental
↓
Data Quality
↓
Delta MERGE
↓
Silver histórica
~~~

La dependencia es por Success.

Si el Copy falla, no quiero procesar Silver.

---

# 30. Problema de capacidad Spark que vi

Durante una ejecución apareció:

~~~text
TooManyRequestsForCapacity
HTTP 430
Failed to create Livy session
~~~

No era un error de mi código.

Fabric no podía iniciar otra sesión Spark porque la capacidad estaba ocupada.

La solución fue liberar sesiones Spark activas y volver a ejecutar.

Esto me recuerda distinguir:

~~~text
error de lógica/código
vs
error de infraestructura/capacidad
~~~

---

# 31. Cómo resumir esta fase en entrevista

> I changed the Bicing ingestion from a single overwritten file to immutable timestamped Bronze snapshots partitioned by date. I then built a PySpark notebook that supports bootstrap and incremental execution. It obtains a watermark from the historical Silver table, selects only newer Bronze files, validates the incremental batch and performs an idempotent Delta MERGE using station_id and snapshot_ingested_at. Finally, I orchestrated the Copy and Notebook activities in Fabric so Bronze-to-Silver runs automatically.

---

# 32. Estado real después de construir Gold

Ya tengo implementado:

~~~text
REST ingestion
Bronze
historical snapshots
PySpark Silver
Data Quality
timestamp normalization
watermark
incremental load
Delta MERGE
idempotency
no-new-data handling
Copy → Notebook orchestration
Gold star schema
dim_station
dim_date
dim_time
fact_bicing_availability
Gold KPIs
Gold referential integrity
Gold analytical validation
~~~

Lo siguiente es:

~~~text
SQL Analytics Endpoint
↓
consultas analíticas
↓
Power BI
↓
documentación visual final
~~~

---

# 33. Qué es una fact table y por qué Gold tiene forma de estrella

Silver contiene una observación histórica completa de cada estación.

Eso significa que en cada snapshot se repiten tanto las medidas como la información descriptiva:

~~~text
station_id
station_name
latitude
longitude
snapshot_ingested_at
free_bikes
empty_slots
ebikes
normal_bikes
station_capacity
...
~~~

En Gold separo dos ideas.

## Fact

La **fact table** contiene lo que ocurrió y lo que puedo medir.

En mi proyecto:

~~~text
gold_fact_bicing_availability
~~~

responde a:

> ¿Qué disponibilidad tenía una estación en un snapshot concreto?

Guarda medidas como:

~~~text
free_bikes
empty_slots
ebikes
normal_bikes
station_capacity
bike_availability_pct
dock_availability_pct
ebike_share_pct
~~~

## Dimension

Las dimensiones describen el contexto del hecho.

~~~text
gold_dim_station
→ qué estación es

gold_dim_date
→ qué día es

gold_dim_time
→ qué hora/franja es
~~~

## Por qué se llama star schema

La fact queda en el centro y las dimensiones alrededor:

~~~text
                    dim_station
                         |
                         |
dim_date -------- fact_bicing -------- dim_time
~~~

Visualmente las relaciones salen desde el centro como las puntas de una estrella.

## Cómo lo explicaría en entrevista

> I modelled Gold as a star schema. The central fact stores historical Bicing availability observations, while station, date and time dimensions provide reusable descriptive context for SQL and BI analysis.

---

# 34. Configuración del notebook Gold

## Código principal

~~~python
SILVER_HISTORY_TABLE = "silver_bicing_station_history"

GOLD_DIM_STATION = "gold_dim_station"
GOLD_DIM_DATE = "gold_dim_date"
GOLD_DIM_TIME = "gold_dim_time"
GOLD_FACT_AVAILABILITY = "gold_fact_bicing_availability"

silver_history_df = spark.table(
    SILVER_HISTORY_TABLE
)
~~~

Uso constantes para que los nombres de tablas estén definidos en un solo sitio.

Si cambio un nombre, no quiero buscar strings repetidos por todo el notebook.

~~~python
spark.table(...)
~~~

lee una tabla registrada del Lakehouse y devuelve un DataFrame.

## Regla importante sobre sesiones Spark

Las tablas Delta permanecen guardadas.

Las variables Python no.

Por eso, si Fabric reinicia la sesión:

~~~text
gold_fact_bicing_availability
→ sigue existiendo como tabla

GOLD_FACT_AVAILABILITY
→ puede desaparecer de memoria
~~~

La solución correcta es ejecutar el notebook desde arriba para volver a crear imports, constantes y DataFrames.

---

# 35. Validar el grain de Silver antes de construir Gold

## Código

~~~python
silver_rows = silver_history_df.count()

silver_unique_grain = (
    silver_history_df
    .select(
        "station_id",
        "snapshot_ingested_at"
    )
    .distinct()
    .count()
)

silver_duplicate_grain = (
    silver_rows - silver_unique_grain
)

assert silver_duplicate_grain == 0,     "Silver historical grain is not unique"
~~~

Antes de modelar Gold vuelvo a comprobar la regla histórica:

~~~text
1 fila
=
1 station_id
+
1 snapshot_ingested_at
~~~

Si Gold parte de una Silver incorrecta, el modelo analítico también será incorrecto.

## Patrón reutilizable

~~~text
total rows
-
distinct business grain
=
duplicate rows
~~~

---

# 36. Construir dim_station con Window y row_number

## Código

~~~python
station_window = (
    Window
    .partitionBy("station_id")
    .orderBy(
        col("snapshot_ingested_at").desc()
    )
)

gold_dim_station_df = (
    silver_history_df
    .withColumn(
        "station_row_number",
        row_number().over(station_window)
    )
    .filter(
        col("station_row_number") == 1
    )
)
~~~

Silver puede contener muchas observaciones de la misma estación.

La dimensión necesita una sola fila por estación.

## partitionBy

~~~python
Window.partitionBy("station_id")
~~~

crea conceptualmente un grupo independiente para cada estación.

## orderBy DESC

~~~python
.orderBy(
    col("snapshot_ingested_at").desc()
)
~~~

ordena cada grupo del snapshot más nuevo al más antiguo.

## row_number

~~~python
row_number().over(station_window)
~~~

numera las filas dentro de cada grupo:

~~~text
station A
snapshot nuevo     1
snapshot anterior  2
snapshot anterior  3
~~~

Luego filtro:

~~~python
col("station_row_number") == 1
~~~

y me quedo con el estado descriptivo más reciente conocido.

## Decisión de modelado

No estoy implementando SCD Type 2 aquí.

Mi fact conserva el histórico de disponibilidad.

Mi dimensión de estación representa la versión descriptiva más reciente.

---

# 37. Surrogate key con xxhash64

## Código

~~~python
xxhash64(
    col("station_id")
).alias("station_key")
~~~

`station_id` pertenece al sistema fuente.

`station_key` pertenece a mi modelo analítico.

Eso permite separar:

~~~text
business / natural key
→ station_id

surrogate analytical key
→ station_key
~~~

## Por qué no uso un número incremental

Si generara IDs basados en el orden de las filas, una reconstrucción podría asignar claves diferentes.

Con:

~~~text
xxhash64(station_id)
~~~

la misma estación produce la misma clave de forma determinista.

Eso encaja bien con mi estrategia de reconstruir Gold.

## Cómo lo explicaría en entrevista

> I preserve the source station_id as the natural key and generate a deterministic station_key with xxhash64 so the Gold model can be rebuilt without changing dimension keys.

---

# 38. Construir dim_date como calendario continuo

Primero obtengo los límites:

~~~python
date_bounds = (
    silver_history_df
    .select(
        spark_min(
            to_date(col("snapshot_ingested_at"))
        ).alias("min_date"),
        spark_max(
            to_date(col("snapshot_ingested_at"))
        ).alias("max_date")
    )
    .first()
)
~~~

Después genero todas las fechas entre ambos extremos:

~~~python
sequence(
    lit(min_date),
    lit(max_date),
    expr("INTERVAL 1 DAY")
)
~~~

`sequence()` crea un array de fechas.

`explode()` convierte ese array en una fila por fecha.

## Por qué no hago solo distinct sobre Silver

Si un día no tuviera snapshots, desaparecería del calendario.

Una dimensión fecha debería poder representar el intervalo completo.

## date_key

~~~python
date_format(
    col("full_date"),
    "yyyyMMdd"
).cast("int")
~~~

convierte:

~~~text
2026-10-04
~~~

en:

~~~text
20261004
~~~

## Atributos reutilizables

La dimensión calcula una vez:

~~~text
year
quarter
month
month_name
day
week_of_year
day_of_week
day_name
is_weekend
~~~

Así no tengo que recalcularlos para cada fila de la fact.

---

# 39. Construir dim_time

Extraigo:

~~~python
hour(
    col("snapshot_ingested_at")
)

minute(
    col("snapshot_ingested_at")
)
~~~

y me quedo con combinaciones distintas.

## time_key

~~~python
hour * 100 + minute
~~~

Ejemplos:

~~~text
08:30 → 830
13:43 → 1343
18:05 → 1805
~~~

## time_label

Uso:

~~~python
concat(
    lpad(col("hour").cast("string"), 2, "0"),
    lit(":"),
    lpad(col("minute").cast("string"), 2, "0")
)
~~~

`lpad` rellena con ceros a la izquierda.

~~~text
8 → "08"
5 → "05"
~~~

## day_period

~~~text
00-05 → Night
06-11 → Morning
12-17 → Afternoon
18-23 → Evening
~~~

Esto prepara una dimensión útil para BI sin repetir esa lógica en cada consulta.

---

# 40. Construir la fact de disponibilidad

La fact parte de Silver y se une a la dimensión de estación para recuperar `station_key`.

~~~python
silver_history_df.join(
    gold_dim_station_df.select(
        "station_key",
        "station_id"
    ),
    on="station_id",
    how="inner"
)
~~~

`station_id` funciona como puente entre Silver y la dimensión.

Después genero:

~~~text
date_key
time_key
~~~

a partir de `snapshot_ingested_at`.

## Grain de la fact

~~~text
1 fila
=
1 estación
+
1 snapshot capturado
~~~

La clave lógica sigue siendo:

~~~text
station_key + snapshot_ingested_at
~~~

## Medidas principales

~~~text
free_bikes
empty_slots
ebikes
normal_bikes
station_capacity
~~~

La fact no existe para describir la estación.

Existe para almacenar el estado medible de la estación en cada momento observado.

---

# 41. KPIs Gold y divisiones seguras

## bike_availability_pct

~~~text
free_bikes
────────────── × 100
station_capacity
~~~

Mide qué porcentaje de la capacidad corresponde a bicicletas disponibles.

## dock_availability_pct

~~~text
empty_slots
────────────── × 100
station_capacity
~~~

Mide qué porcentaje de la estación está disponible para devolver bicicletas.

## ebike_share_pct

~~~text
ebikes
────────── × 100
free_bikes
~~~

Mide qué parte de las bicicletas disponibles son eléctricas.

## Por qué uso when

~~~python
when(
    col("station_capacity") > 0,
    ...
)
~~~

No quiero dividir entre cero.

Además, dejar NULL cuando el denominador no permite calcular el porcentaje es semánticamente mejor que inventar 0%.

~~~text
0%
→ el KPI fue calculable y dio cero

NULL
→ no era calculable
~~~

Esta diferencia es importante en analítica.

---

# 42. Quality checks de Gold

Antes de persistir Gold valido varias reglas.

## 1. Row reconciliation

~~~python
assert fact_rows == silver_rows
~~~

Quiero asegurarme de que transformar Silver a la fact no haya perdido ni multiplicado observaciones.

## 2. Grain único

~~~text
station_key + snapshot_ingested_at
~~~

debe seguir siendo único.

## 3. station_key único en la dimensión

Una dimensión no puede tener dos filas con la misma clave si espero una relación many-to-one desde la fact.

## 4. Foreign keys no nulas

Compruebo:

~~~text
station_key
date_key
time_key
~~~

## 5. Referential integrity

Uso left anti join.

~~~python
fact_keys.join(
    dimension_keys,
    on="station_key",
    how="left_anti"
)
~~~

Eso devuelve claves que existen en la fact pero no en la dimensión.

El resultado debe ser:

~~~text
0 orphan keys
~~~

Repito el patrón para estación, fecha y hora.

## 6. KPIs entre 0 y 100

Las medidas porcentuales no nulas deben respetar:

~~~text
0 <= KPI <= 100
~~~

## Cómo lo explicaría en entrevista

> Before writing Gold I validate row reconciliation, fact grain uniqueness, dimension-key uniqueness, non-null foreign keys, referential integrity with left anti joins and KPI ranges.

---

# 43. Persistir Gold y por qué uso overwrite

Escribo cuatro tablas Delta:

~~~text
gold_dim_station
gold_dim_date
gold_dim_time
gold_fact_bicing_availability
~~~

El patrón es:

~~~python
df.write
.format("delta")
.mode("overwrite")
.option("overwriteSchema", "true")
.saveAsTable(...)
~~~

## Por qué Gold se sobrescribe pero Silver es incremental

Silver es mi histórico confiable y crece incrementalmente.

Gold es una representación analítica derivada.

Actualmente hago:

~~~text
Silver completa y validada
↓
recalcular dimensiones
↓
recalcular fact
↓
overwrite Gold
~~~

Eso tiene sentido porque el volumen todavía es pequeño.

También me permite cambiar una fórmula de KPI y reconstruir todo Gold con una única definición coherente.

Si tuviera cientos de millones o miles de millones de filas, podría plantear Gold incremental.

---

# 44. Validación analítica del star schema

Después de escribir Gold, vuelvo a leer las tablas.

~~~python
gold_fact_df = spark.table(
    GOLD_FACT_AVAILABILITY
)
~~~

Luego uno:

~~~text
fact
+ dim_station
+ dim_date
+ dim_time
~~~

y comparo:

~~~text
Fact rows
Joined analytical rows
~~~

No quiero que un join de dimensiones multiplique accidentalmente las observaciones.

## Primera pregunta analítica

Agrupo por:

~~~text
day_period
~~~

y calculo:

~~~text
observations
avg_free_bikes
avg_bike_availability_pct
avg_dock_availability_pct
~~~

Ya no estoy limpiando JSON.

Estoy usando el modelo para responder preguntas.

## Segunda pregunta analítica

Filtro estaciones online y agrupo por estación para ordenar:

~~~text
avg_bike_availability_pct ASC
~~~

Eso permite identificar estaciones con menor disponibilidad media durante las observaciones.

---

# 45. Por qué no uní todavía Bicing con district_context

Bicing tiene:

~~~text
station_id
station_name
latitude
longitude
~~~

El dataset contextual tiene:

~~~text
district_code
neighborhood_code
district_name
neighborhood_name
...
~~~

No existe actualmente una clave fiable como:

~~~text
station_id ↔ district_code
~~~

Tampoco tengo en ese dataset contextual una geometría lista para hacer un spatial join.

Por eso no debo inventar una relación por nombre.

La decisión correcta es:

~~~text
no join artificial
↓
mantener contexto separado
↓
añadir spatial enrichment real más adelante si aporta valor
~~~

Esto también es una decisión de ingeniería defendible.

---

# 46. Estado actual del proyecto después de Gold

Mi arquitectura implementada ahora es:

~~~text
Public REST APIs
↓
Fabric Data Factory
↓
Bronze / OneLake
↓
historical timestamped snapshots
↓
PySpark
↓
Silver Delta
↓
watermark + incremental processing
↓
Delta MERGE
↓
Gold star schema
├── dim_station
├── dim_date
├── dim_time
└── fact_bicing_availability
↓
analytical validation
~~~

Ya tengo también implementada la capa SQL Analytics Endpoint sobre Gold.

Lo siguiente es:

~~~text
Power BI
↓
dashboard analítico
↓
documentación visual final
~~~

## Resumen de entrevista de esta fase

> I built a Gold analytical layer on top of the historical Silver Bicing table. I modelled it as a star schema with station, date and time dimensions around an availability fact table. I use a deterministic station surrogate key, calculate business-facing availability KPIs, validate referential integrity and row reconciliation, and persist the model as Delta tables. For the current volume, Gold is rebuilt from Silver so the analytical layer remains simple and consistent.

---

# 47. SQL Analytics Endpoint: para qué lo uso

Después de construir Gold con Spark, no necesito mover las tablas a otra base de datos para consultarlas con SQL.

Las tablas Delta Gold aparecen en el SQL Analytics Endpoint del Lakehouse:

~~~text
gold_dim_station
gold_dim_date
gold_dim_time
gold_fact_bicing_availability
~~~

Esto me permite consumir el mismo modelo con T-SQL.

La arquitectura queda:

~~~text
PySpark
↓
Gold Delta
↓
SQL Analytics Endpoint
↓
T-SQL
↓
Power BI
~~~

## Idea importante

Spark y SQL no están trabajando sobre dos copias independientes de Gold.

Estoy exponiendo las mismas tablas Delta mediante otra interfaz de consumo.

## Cómo lo explicaría en entrevista

> I build and validate the Gold layer with PySpark and then expose the same Delta tables through the Fabric SQL Analytics Endpoint for T-SQL analysis and BI consumption.

---

# 48. SELECT, aliases y TOP

Mi primera consulta SQL importante fue:

~~~sql
SELECT TOP 50
    s.station_name,
    d.full_date,
    t.time_label,
    t.day_period,
    f.free_bikes,
    f.empty_slots,
    f.bike_availability_pct

FROM gold_fact_bicing_availability AS f
...
~~~

## SELECT

Elige las columnas que quiero devolver.

~~~sql
SELECT station_name
~~~

es conceptualmente parecido a:

~~~python
df.select("station_name")
~~~

en PySpark.

## TOP

~~~sql
TOP 50
~~~

limita el resultado a 50 filas.

Lo uso para inspección rápida sin devolver toda la tabla.

## Alias de tabla

~~~sql
gold_fact_bicing_availability AS f
gold_dim_station AS s
gold_dim_date AS d
gold_dim_time AS t
~~~

Esto me permite escribir:

~~~sql
f.free_bikes
s.station_name
~~~

en vez de repetir el nombre completo de cada tabla.

---

# 49. JOIN del star schema en SQL

La consulta principal reconstruye el contexto analítico:

~~~sql
FROM gold_fact_bicing_availability AS f

LEFT JOIN gold_dim_station AS s
    ON f.station_key = s.station_key

LEFT JOIN gold_dim_date AS d
    ON f.date_key = d.date_key

LEFT JOIN gold_dim_time AS t
    ON f.time_key = t.time_key
~~~

La fact guarda claves y medidas.

Las dimensiones convierten esas claves en contexto legible:

~~~text
station_key
→ station_name / latitude / longitude

date_key
→ full_date / day_name / is_weekend

time_key
→ time_label / day_period
~~~

## Por qué LEFT JOIN

Con LEFT JOIN conservo todas las filas de la fact aunque una dimensión no encuentre coincidencia.

Si eso ocurriera:

~~~text
fact row
→ permanece

dimension columns
→ NULL
~~~

Con INNER JOIN esa fila desaparecería.

En mi modelo no debería haber huérfanos porque ya validé referential integrity en Gold, pero LEFT JOIN hace que un problema sea más visible en vez de ocultarlo mediante pérdida de filas.

---

# 50. ORDER BY, ASC y DESC

En la preview uso:

~~~sql
ORDER BY
    d.full_date DESC,
    t.time_key DESC,
    s.station_name;
~~~

Esto significa:

~~~text
fecha más nueva primero
↓
hora más nueva primero
↓
estaciones por nombre ascendente
~~~

## ASC

Orden ascendente.

Es el valor por defecto.

~~~sql
ORDER BY station_name
~~~

equivale a:

~~~sql
ORDER BY station_name ASC
~~~

## DESC

Orden descendente.

~~~sql
ORDER BY full_date DESC
~~~

pone primero las fechas más recientes.

---

# 51. GROUP BY y agregaciones

Para encontrar estaciones con peor disponibilidad media uso:

~~~sql
SELECT TOP 20
    s.station_name,
    COUNT(*) AS observations,
    AVG(f.bike_availability_pct)
        AS avg_bike_availability_pct

FROM gold_fact_bicing_availability AS f

LEFT JOIN gold_dim_station AS s
    ON f.station_key = s.station_key

WHERE f.is_online = 1

GROUP BY
    s.station_name

ORDER BY
    avg_bike_availability_pct ASC;
~~~

## GROUP BY

Agrupa varias filas que pertenecen a la misma entidad.

Ejemplo:

~~~text
Station A  10%
Station A  30%
Station A  20%
~~~

con:

~~~sql
GROUP BY station_name
~~~

se convierte conceptualmente en un grupo:

~~~text
Station A
→ 3 observaciones
~~~

Después puedo aplicar agregaciones al grupo.

## COUNT(*)

~~~sql
COUNT(*)
~~~

cuenta las filas del grupo.

En mi caso:

~~~text
observations
~~~

me dice cuántos snapshots válidos estoy usando para calcular la media de esa estación.

## AVG

~~~sql
AVG(f.bike_availability_pct)
~~~

calcula el promedio del KPI dentro de cada grupo.

---

# 52. WHERE: filtrar antes de agrupar

Uso:

~~~sql
WHERE f.is_online = 1
~~~

para excluir observaciones offline antes de calcular el promedio.

Conceptualmente:

~~~text
todas las filas
↓
WHERE
↓
solo online
↓
GROUP BY station
↓
AVG
~~~

Esto evita mezclar estados offline con la disponibilidad normal de la estación.

## Equivalencia mental con PySpark

SQL:

~~~sql
WHERE f.is_online = 1
~~~

PySpark:

~~~python
.filter(
    col("is_online") == True
)
~~~

---

# 53. HAVING: filtrar después de agrupar

Aunque la consulta de HAVING fue principalmente de aprendizaje y no la guardé en el repo profesional, el concepto sí debo dominar.

Ejemplo:

~~~sql
GROUP BY
    s.station_name

HAVING COUNT(*) >= 5
~~~

Significa:

> Agrupa primero por estación y después conserva solo estaciones con al menos 5 observaciones.

No puedo poner:

~~~sql
WHERE COUNT(*) >= 5
~~~

porque WHERE ocurre antes del GROUP BY.

## Orden conceptual SQL que debo recordar

~~~text
FROM / JOIN
↓
WHERE
↓
GROUP BY
↓
HAVING
↓
SELECT
↓
ORDER BY
~~~

No es necesariamente el orden físico interno exacto del motor, pero es una buena regla mental para entender por qué una expresión está disponible o no en cada fase.

---

# 54. Disponibilidad por franja del día

Consulta:

~~~sql
SELECT
    t.day_period,
    COUNT(*) AS observations,
    AVG(f.free_bikes) AS avg_free_bikes,
    AVG(f.bike_availability_pct)
        AS avg_bike_availability_pct,
    AVG(f.dock_availability_pct)
        AS avg_dock_availability_pct

FROM gold_fact_bicing_availability AS f

LEFT JOIN gold_dim_time AS t
    ON f.time_key = t.time_key

GROUP BY
    t.day_period;
~~~

Aquí aprovecho una ventaja del modelo dimensional.

No recalculo:

~~~text
si hora < 6 → Night
si hora < 12 → Morning
...
~~~

en cada consulta.

Ya lo resolví una vez en:

~~~text
gold_dim_time.day_period
~~~

Eso mantiene la lógica analítica centralizada.

---

# 55. Weekday vs weekend

Consulta:

~~~sql
SELECT
    d.is_weekend,
    COUNT(*) AS observations,
    AVG(f.free_bikes) AS avg_free_bikes,
    AVG(f.empty_slots) AS avg_empty_slots,
    AVG(f.bike_availability_pct)
        AS avg_bike_availability_pct,
    AVG(f.dock_availability_pct)
        AS avg_dock_availability_pct

FROM gold_fact_bicing_availability AS f

LEFT JOIN gold_dim_date AS d
    ON f.date_key = d.date_key

GROUP BY
    d.is_weekend;
~~~

Otra vez reutilizo un atributo ya creado en la dimensión:

~~~text
dim_date.is_weekend
~~~

La fact no necesita guardar repetidamente si cada fecha era fin de semana.

---

# 56. Disponibilidad por día de la semana

Consulta:

~~~sql
SELECT
    d.day_name,
    d.day_of_week,
    COUNT(*) AS observations,
    AVG(f.bike_availability_pct)
        AS avg_bike_availability_pct,
    AVG(f.dock_availability_pct)
        AS avg_dock_availability_pct

FROM gold_fact_bicing_availability AS f

LEFT JOIN gold_dim_date AS d
    ON f.date_key = d.date_key

GROUP BY
    d.day_name,
    d.day_of_week

ORDER BY
    d.day_of_week;
~~~

## Por qué agrupo también por day_of_week

Quiero mostrar:

~~~text
Monday
Tuesday
Wednesday
...
Sunday
~~~

y no:

~~~text
Friday
Monday
Saturday
...
~~~

ordenado alfabéticamente.

Por eso la dimensión fecha contiene un número de orden reutilizable.

---

# 57. CASE WHEN: IF / ELSE de SQL

En la query preparada para Power BI añadí:

~~~sql
CASE
    WHEN f.is_online = 0
        THEN 'Offline'

    WHEN f.station_capacity IS NULL
         OR f.station_capacity = 0
        THEN 'No capacity'

    WHEN f.bike_availability_pct < 20
        THEN 'Low bikes'

    WHEN f.dock_availability_pct < 20
        THEN 'Low docks'

    ELSE 'Balanced'
END AS availability_status
~~~

CASE evalúa condiciones en orden.

La primera condición que se cumple gana.

## Por qué el orden importa

Si una estación está offline y además tiene disponibilidad baja:

~~~text
is_online = 0
bike_availability_pct = 0
~~~

quiero:

~~~text
Offline
~~~

y no:

~~~text
Low bikes
~~~

Por eso compruebo offline primero.

## Clasificaciones actuales

~~~text
Offline
No capacity
Low bikes
Low docks
Balanced
~~~

El umbral del 20% es una regla analítica del serving/dashboard.

No es una corrección del dato fuente y no pertenece a la lógica de calidad Silver.

---

# 58. Serving query para Power BI

La consulta:

~~~text
06_powerbi_serving_query.sql
~~~

junta:

~~~text
dim_station
+
dim_date
+
dim_time
+
fact_bicing_availability
~~~

y devuelve una tabla plana con:

~~~text
station_name
latitude
longitude
full_date
day_name
is_weekend
time_label
day_period
snapshot_ingested_at
free_bikes
empty_slots
ebikes
normal_bikes
station_capacity
bike_availability_pct
dock_availability_pct
ebike_share_pct
is_online
availability_status
~~~

## Por qué tengo star schema si luego hago una tabla plana

El modelo físico sigue siendo dimensional.

La query plana es una vista de consumo.

~~~text
modelo Gold bien diseñado
↓
query serving
↓
consumidor sencillo
~~~

No son ideas contradictorias.

La estrella facilita modelado y reutilización.

La query plana facilita algunos escenarios de BI y presentación.

---

# 59. Mis seis queries SQL del proyecto

En el repo profesional guardé:

~~~text
01_gold_star_schema_preview.sql
02_low_availability_stations.sql
03_availability_by_day_period.sql
04_weekday_vs_weekend.sql
05_availability_by_day.sql
06_powerbi_serving_query.sql
~~~

Cada una tiene una intención.

~~~text
01 → validar/mostrar star schema
02 → ranking de estaciones
03 → análisis por franja
04 → weekday vs weekend
05 → análisis por día
06 → serving para Power BI
~~~

La consulta de HAVING fue de práctica y no necesito subirla al repo principal.

---

# 60. Estado del proyecto después de SQL

Ahora tengo implementado:

~~~text
REST ingestion
↓
Bronze
↓
historical snapshots
↓
Silver incremental
↓
watermark
↓
Delta MERGE
↓
Gold star schema
↓
Gold quality checks
↓
SQL Analytics Endpoint
↓
T-SQL joins
↓
aggregations
↓
serving query
~~~

Lo siguiente es:

~~~text
Power BI
↓
dashboard
↓
evidencia final
↓
arquitectura/documentación visual
~~~

## Resumen de entrevista de esta fase

> I expose the Gold Delta tables through the Fabric SQL Analytics Endpoint and use T-SQL to query the star schema. I built analytical queries for station availability, time-of-day and weekday/weekend comparisons, and a serving query that joins the dimensions with the fact and derives a business-facing availability status using CASE logic. This SQL layer is the serving surface for the Power BI phase.
