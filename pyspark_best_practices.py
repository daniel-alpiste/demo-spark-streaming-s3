# Databricks notebook source
# MAGIC %md
# MAGIC # Buenas Prácticas en PySpark: Guía Completa de Optimización
# MAGIC
# MAGIC Este notebook presenta una guía detallada de las mejores prácticas para optimizar el rendimiento en PySpark, con ejemplos prácticos y explicaciones detalladas.
# MAGIC
# MAGIC ## Contenido
# MAGIC 1. Transformaciones Eficientes
# MAGIC 2. Operaciones de Join
# MAGIC 3. Manejo de Memoria y Caché
# MAGIC 4. Operaciones de Agregación
# MAGIC 5. Procesamiento por Particiones
# MAGIC 6. Conversiones a Pandas
# MAGIC 7. Consejos Adicionales

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Transformaciones Eficientes
# MAGIC ### selectExpr() vs withColumn()

# COMMAND ----------

# Configuración inicial
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Crear datos de ejemplo
data = [(1, "John", 30), (2, "Alice", 25), (3, "Bob", 35)]
df = spark.createDataFrame(data, ["id", "name", "age"])

# COMMAND ----------

# MAGIC %md
# MAGIC #### ❌ Menos Óptimo: Usando withColumn()

# COMMAND ----------

# Ejemplo menos óptimo
result_less_optimal = (
    df.withColumn("age_plus_10", F.col("age") + 10)
    .withColumn("name_upper", F.upper(F.col("name")))
    .withColumn("is_adult", F.col("age") >= 18)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ✅ Más Óptimo: Usando selectExpr()

# COMMAND ----------

# Ejemplo óptimo
result_optimal = df.selectExpr(
    "*", "age + 10 as age_plus_10", "upper(name) as name_upper", "age >= 18 as is_adult"
)

# COMMAND ----------

# MAGIC %md
# MAGIC **¿Por qué es mejor?**
# MAGIC - `selectExpr()` evalúa todas las transformaciones en una sola pasada
# MAGIC - Reduce el número de transformaciones en el plan de ejecución
# MAGIC - Más legible y mantenible
# MAGIC - Permite usar expresiones SQL que son más optimizables por Catalyst

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Operaciones de Join
# MAGIC ### broadcast() vs join() regular

# COMMAND ----------

# Crear dataframes de ejemplo
customers = spark.createDataFrame(
    [(1, "John"), (2, "Alice"), (3, "Bob")], ["customer_id", "name"]
)

orders = spark.createDataFrame(
    [(1, 100), (1, 200), (2, 150), (3, 300)], ["customer_id", "amount"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ❌ Menos Óptimo: Join Regular

# COMMAND ----------

result_regular = customers.join(orders, "customer_id")

# COMMAND ----------

# MAGIC %md
# MAGIC #### ✅ Más Óptimo: Broadcast Join

# COMMAND ----------

from pyspark.sql.functions import broadcast

result_broadcast = customers.join(broadcast(orders), "customer_id")

# COMMAND ----------

# MAGIC %md
# MAGIC **¿Por qué es mejor?**
# MAGIC - Evita el shuffle de datos entre nodos
# MAGIC - Ideal cuando una tabla es significativamente más pequeña (<10GB)
# MAGIC - Reduce el tráfico de red
# MAGIC - Mejora dramáticamente el rendimiento en joins con tablas pequeñas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Manejo de Memoria y Caché
# MAGIC ### persist() vs cache()

# COMMAND ----------

# MAGIC %md
# MAGIC #### ❌ Menos Óptimo: cache()

# COMMAND ----------

df_cached = df.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC #### ✅ Más Óptimo: persist()

# COMMAND ----------

from pyspark.storagelevel import StorageLevel

df_persisted = df.persist(StorageLevel.MEMORY_AND_DISK)

# COMMAND ----------

# MAGIC %md
# MAGIC **¿Por qué es mejor?**
# MAGIC - `persist()` ofrece control granular sobre el nivel de almacenamiento
# MAGIC - Permite especificar si usar memoria, disco o ambos
# MAGIC - Puede configurar serialización y replicación
# MAGIC - `cache()` es equivalente a `persist(StorageLevel.MEMORY_ONLY)`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Operaciones de Agregación
# MAGIC ### groupBy().agg() vs Múltiples groupBy()

# COMMAND ----------

# MAGIC %md
# MAGIC #### ❌ Menos Óptimo: Múltiples groupBy()

# COMMAND ----------

# Ejemplo menos óptimo
avg_age = df.groupBy("name").agg(F.avg("age").alias("avg_age"))
max_age = df.groupBy("name").agg(F.max("age").alias("max_age"))
count = df.groupBy("name").count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### ✅ Más Óptimo: Single groupBy().agg()

# COMMAND ----------

# Ejemplo óptimo
metrics = df.groupBy("name").agg(
    F.avg("age").alias("avg_age"),
    F.max("age").alias("max_age"),
    F.count("*").alias("count"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC **¿Por qué es mejor?**
# MAGIC - Una sola operación de agrupación
# MAGIC - Reduce el número de jobs y stages
# MAGIC - Mejor utilización de recursos
# MAGIC - Evita múltiples lecturas de los mismos datos

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Procesamiento por Particiones
# MAGIC ### mapPartitions() vs map()

# COMMAND ----------

# MAGIC %md
# MAGIC #### ❌ Menos Óptimo: map()

# COMMAND ----------


def process_row(row):
    return row.age * 2


result_map = df.rdd.map(process_row)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ✅ Más Óptimo: mapPartitions()

# COMMAND ----------


def process_partition(iterator):
    # Inicializar recursos una vez por partición
    processed = []
    for row in iterator:
        processed.append(row.age * 2)
    return iter(processed)


result_partition = df.rdd.mapPartitions(process_partition)

# COMMAND ----------

# MAGIC %md
# MAGIC **¿Por qué es mejor?**
# MAGIC - Reduce el overhead de función por registro
# MAGIC - Permite inicializar recursos una vez por partición
# MAGIC - Mejor para operaciones que requieren contexto o estado
# MAGIC - Más eficiente para operaciones en lote

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Conversiones a Pandas
# MAGIC ### limit().toPandas() vs toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### ❌ Menos Óptimo: toPandas() completo

# COMMAND ----------

# ¡Peligroso con grandes datasets!
pdf_full = df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### ✅ Más Óptimo: limit().toPandas()

# COMMAND ----------

# Más seguro
pdf_limited = df.limit(1000).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC **¿Por qué es mejor?**
# MAGIC - Evita problemas de memoria en el driver
# MAGIC - Más seguro para grandes datasets
# MAGIC - Mejor para análisis exploratorio
# MAGIC - Previene OutOfMemoryError

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Consejos Adicionales

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Optimización de Filtros
# MAGIC ```python
# MAGIC # ✅ Óptimo: Filtrar antes de join
# MAGIC result = df1.filter(condition).join(df2, "key")
# MAGIC
# MAGIC # ❌ Menos Óptimo: Filtrar después de join
# MAGIC result = df1.join(df2, "key").filter(condition)
# MAGIC ```
# MAGIC
# MAGIC ### 2. Uso de countApprox()
# MAGIC ```python
# MAGIC # ✅ Óptimo: Para grandes datasets
# MAGIC count_approx = df.countApprox(timeout=1000)
# MAGIC
# MAGIC # ❌ Menos Óptimo: Para grandes datasets
# MAGIC count_exact = df.count()
# MAGIC ```
# MAGIC
# MAGIC ### 3. Uso de foreachPartition()
# MAGIC ```python
# MAGIC # ✅ Óptimo: Procesar por particiones
# MAGIC def process_partition(iterator):
# MAGIC     # Inicializar conexión una vez por partición
# MAGIC     for record in iterator:
# MAGIC         # Procesar registro
# MAGIC         pass
# MAGIC
# MAGIC df.foreachPartition(process_partition)
# MAGIC
# MAGIC # ❌ Menos Óptimo: Procesar registro por registro
# MAGIC def process_record(record):
# MAGIC     # Inicializar conexión para cada registro
# MAGIC     pass
# MAGIC
# MAGIC df.foreach(process_record)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen de Mejores Prácticas
# MAGIC
# MAGIC | Operación | Práctica Óptima | Práctica Menos Óptima | Beneficio Principal |
# MAGIC |-----------|-----------------|---------------------|-------------------|
# MAGIC | Transformaciones | `selectExpr()` | `withColumn()` | Evaluación en una pasada |
# MAGIC | Joins | `broadcast()` | `join()` regular | Evita shuffle |
# MAGIC | Caché | `persist()` | `cache()` | Control granular |
# MAGIC | Agregaciones | `groupBy().agg()` | Múltiples `groupBy()` | Reduce operaciones |
# MAGIC | Procesamiento | `mapPartitions()` | `map()` | Reduce overhead |
# MAGIC | Conversión a Pandas | `limit().toPandas()` | `toPandas()` completo | Seguridad de memoria |
# MAGIC | Filtrado | Antes de join | Después de join | Reduce datos temprano |
# MAGIC | Conteo | `countApprox()` | `count()` | Eficiencia en recursos |
# MAGIC | Procesamiento por registros | `foreachPartition()` | `foreach()` | Mejor rendimiento |
