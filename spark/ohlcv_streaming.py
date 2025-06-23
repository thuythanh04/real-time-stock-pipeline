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

# Load Avro schema from file
with open(AVRO_SCHEMA_PATH, "r") as f:
  avro_schema = f.read()

# Read from Kafka
kafka_df = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("failOnDataLoss", "false") \
        .load()

# Decode Avro
decoded_df = kafka_df.select(from_avro(col("value"), avro_schema).alias("data"))

# Explode array of trades
exploded_df = decoded_df.selectExpr("data.type", "explode(data.data) as tick")

# Extract and transform fields
ticks_df = exploded_df.select(
    col("tick.s").alias("symbol"),
    col("tick.p").alias("price"),
    (col("tick.t") / 1000).cast("timestamp").alias("event_time"),
    col("tick.v").alias("volume")
)

# Aggregate to OHLCV format with 1-minute window and watermark
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

# Helper function to parse JDBC URL to extract host, port, and database name
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
    if df.isEmpty():
        print(f"Batch {epoch_id}: DataFrame is empty, skipping write to DB.")
        return

    print(f"Batch {epoch_id}: Processing {df.count()} records for UPSERT...")

    pandas_df = df.toPandas()

    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_DATABASE,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cur = conn.cursor()
        
        # UPSERT logic using ON CONFLICT (PostgreSQL-specific)
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

        for _, row in pandas_df.iterrows():
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
        conn.commit()
        print(f"Batch {epoch_id}: Successfully upserted {len(pandas_df)} records.")

    except psycopg2.Error as e:
        if conn:
            conn.rollback()
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