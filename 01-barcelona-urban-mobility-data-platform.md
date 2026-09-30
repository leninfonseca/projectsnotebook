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

Actualmente todavía me falta:

- una fuente principal realmente orientada a movilidad;
- incremental loading;
- watermark;
- Delta MERGE/upsert;
- Silver enrichment entre distintas fuentes;
- Gold;
- modelo dimensional;
- SQL analítico;
- orquestación completa;
- visualización final.

Puedo explicar que forman parte de la arquitectura objetivo, pero debo separar claramente **implementado** de **planificado**.

---

# 14. Resumen de 30 segundos

> Construí una ingesta en Microsoft Fabric desde la API CKAN de Open Data Barcelona hacia una capa Bronze en OneLake. Después utilicé un notebook PySpark para leer el JSON anidado, extraer y explotar result.records, normalizar nombres y tipos, añadir metadata y ejecutar controles automáticos de calidad. Finalmente persistí el resultado como una Delta table Silver y la volví a leer para validar schema y row count.

Ese resumen no sustituye entender el código. El objetivo de estas notas es que pueda explicar qué ocurre debajo de cada frase.
