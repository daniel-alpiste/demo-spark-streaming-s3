from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("action", StringType(), True),
    StructField("value", DoubleType(), True)
])

json_path = "s3a://test-streaming-12354325/streaming_jsons/"
schema_path = "dbfs:/tmp/streaming_schema/"

df_stream = (spark.readStream
                  .format("cloudFiles")                   
                  .option("cloudFiles.format", "json")     
                  .option("cloudFiles.inferSchema", "true") 
                  .option("cloudFiles.schemaLocation", schema_path)
                  .schema(schema)
                  .load(json_path))

checkpoint_path = "dbfs:/mnt/path_to_checkpoint/json_events_checkpoint"

query = (
    df_stream
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .table("workspace.streaming_test.json_events_delta")  # <-- En vez de .start("ruta...")
)

query.awaitTermination()