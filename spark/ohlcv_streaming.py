from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, window, first, last, max, min, sum
from pyspark.sql.avro.functions import from_avro
import json
import re
import psycopg2

# Load config
with open("config.json", "r") as f:
    config = json.load(f)
    
KAFKA_BOOTSTRAP = config["kafka_bootstrap_servers"]
KAFKA_TOPIC = config["kafka_topic"]
AVRO_SCHEMA_PATH = config["schema_path"]
DB_URL = config["db_url"]
DB_USER = config["db_user"]
DB_PASSWORD = config["db_password"]

# Define Spark Session
spark = SparkSession.builder\
      .appName("OHLCV Aggregation")\
      .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

with open(AVRO_SCHEMA_PATH, "r") as f:
  avro_schema_str = f.read()


# Read from Kafka
raw_df = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("failOnDataLoss", "false") \
        .load()


# Decode Avro payload
parsed_df = raw_df.select(
    from_avro(col("value"), avro_schema_str).alias("parsed")
).select(
    explode(col("parsed.data")).alias("tick")
)

# Extract and transform fields
ticks_df = parsed_df.select(
    col("tick.s").alias("symbol"),
    col("tick.p").alias("price"),
    (col("tick.t") / 1000).cast("timestamp").alias("event_time"),
    col("tick.v").alias("volume")
)

# OHLCV aggregation
ohlcv_df = ticks_df \
    .withWatermark("event_time", "1 minute") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("symbol")
    ).agg(
        first("price").alias("open"),
        max("price").alias("high"),
        min("price").alias("low"),
        last("price").alias("close"),
        sum("volume").alias("volume")
    ).select(
        col("window.start").alias("timestamp"),
        "symbol", "open", "high", "low", "close", "volume"
    )

# Function to extract host, port, and database from JDBC URL for psycopg2
def parse_jdbc_url(jdbc_url):
    match = re.match(r"jdbc:postgresql://([^:]+):(\d+)/([^?]+)", jdbc_url)
    if match:
        host = match.group(1)
        port = match.group(2)
        database = match.group(3)
        return host, port, database
    raise ValueError(f"Invalid JDBC URL format for PostgreSQL: {jdbc_url}")

# Parse DB_URL for psycopg2 connection
try:
    DB_HOST, DB_PORT, DB_DATABASE = parse_jdbc_url(DB_URL)
except ValueError as e:
    print(f"Failed to parse DB_URL: {e}")
    exit(1)


# Function to write OHLCV data to PostgreSQL with UPSERT logic
def write_ohlcv_to_pg(df, epoch_id):
    # Check if the DataFrame is empty to avoid unnecessary database operations
    if df.isEmpty():
        print(f"Batch {epoch_id}: DataFrame is empty, skipping write to DB.")
        return

    print(f"Batch {epoch_id}: Processing {df.count()} records for UPSERT...")

    # Convert Spark DataFrame to Pandas DataFrame to iterate over rows easily
    # This collects data to the driver, which can be a bottleneck for very large batches.
    # For production, consider using more advanced Spark-native JDBC upsert solutions
    # if batch sizes are consistently very large.
    pandas_df = df.toPandas()

    conn = None
    try:
        # Establish a new PostgreSQL connection for each batch
        # This ensures transaction isolation for each micro-batch
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_DATABASE,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()

        # Iterate over each row in the Pandas DataFrame
        for index, row in pandas_df.iterrows():
            # SQL UPSERT statement for PostgreSQL
            # It assumes the 'ohlcv_data' table has a UNIQUE constraint on (timestamp, symbol)
            upsert_sql = """
                INSERT INTO ohlcv_data (timestamp, symbol, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (timestamp, symbol) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume;
            """
            cur.execute(
                upsert_sql,
                (
                    row["timestamp"],
                    row["symbol"],
                    row["open"],
                    row["high"],
                    row["low"],
                    row["close"],
                    row["volume"]
                )
            )
        conn.commit() # Commit all changes for the batch
        print(f"Batch {epoch_id}: Successfully upserted {len(pandas_df)} records.")

    except psycopg2.Error as e:
        if conn:
            conn.rollback() # Rollback on error
        print(f"Batch {epoch_id}: Database error during UPSERT: {e}")
    except Exception as e:
        print(f"Batch {epoch_id}: An unexpected error occurred: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

# Start the main streaming query (to PostgreSQL)
query = ohlcv_df.writeStream \
    .foreachBatch(write_ohlcv_to_pg) \
    .outputMode("update") \
    .option("checkpointLocation", "chk/ohlcv") \
    .start()\
    .awaitTermination()