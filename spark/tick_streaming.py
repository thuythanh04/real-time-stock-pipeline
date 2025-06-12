from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import *
import json

# Load config
with open("config.json", "r") as f:
    config = json.load(f)

KAFKA_BOOTSTRAP = config["kafka_bootstrap_servers"]
KAFKA_TOPIC = config["kafka_topic"]
AVRO_SCHEMA_PATH = config["schema_path"]
DB_URL = config["db_url"]
DB_USER = config["db_user"]
DB_PASSWORD = config["db_password"]

# Define Spark session
spark = SparkSession.builder \
    .appName("Tick Stream to TimescaleDB") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Load Avro schema from file
with open(AVRO_SCHEMA_PATH, "r") as f:
    avro_schema = f.read()

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("failOnDataLoss", "false") \
    .load()

# Decode Avro
decoded_df = kafka_df.select(from_avro(col("value"), avro_schema).alias("data"))

# Explode array of trades
exploded_df = decoded_df.selectExpr("data.type", "explode(data.data) as tick")

# Flatten fields
ticks_df = exploded_df.select(
    col("tick.s").alias("symbol"),
    col("tick.p").alias("price"),
    (col("tick.t") / 1000).cast("timestamp").alias("event_time"),
    col("tick.v").alias("volume")
)

# Write to TimescaleDB using JDBC
query = ticks_df.writeStream \
    .outputMode("append") \
    .foreachBatch(lambda batch_df, _: batch_df.write \
        .format("jdbc") \
        .option("url", DB_URL) \
        .option("dbtable", "tick_data") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    ) \
    .option("checkpointLocation", "chk/tick") \
    .start()

query.awaitTermination()
