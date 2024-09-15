import pyspark
import os
from dotenv import load_dotenv
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.window import Window

dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

spark_hostname = os.getenv("SPARK_MASTER_HOST_NAME")
spark_port = os.getenv("SPARK_MASTER_PORT")
kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")
postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_port = os.getenv('POSTGRES_PORT')
postgres_dw_db = os.getenv('POSTGRES_DW_DB')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')
postgres_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_dw_db}"
spark_host = f"spark://{spark_hostname}:{spark_port}"

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 org.postgresql:postgresql:42.2.18"
)


spark = (
    pyspark.sql.SparkSession.builder.appName("DibimbingStreaming")
    .master(spark_host)
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")
    .config("spark.sql.shuffle.partitions", 4)
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", True)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType(
    [

        StructField("TransactionId", StringType(), True),
        StructField("UserId", IntegerType(), True),
        StructField("ItemName", StringType(), True),
        StructField("ItemColor", StringType(), True),
        StructField("NumberOfItemPurchased", IntegerType(), True),
        StructField("PricePerltem", IntegerType(), True),
        StructField("ts", TimestampType(), True),
    ]
)

stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{kafka_host}:9092")
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .load()
)


parsed_df = stream_df.selectExpr("CAST(value AS STRING)").select(F.from_json(F.col("value"), schema).alias("data")).select("data.*")

# Fill Null with zero

retail_data = parsed_df.fillna(0, subset=["NumberOfItemPurchased", "PricePerltem"])


# Window Aggregation with Watermark

windowed_retail = (
    retail_data.withWatermark("ts", "1 minutes")  # 10-minute late data tolerance
    .groupBy(
        F.window("ts", "1 minutes", "1 minutes"),
        "ItemName", 
        "ItemColor",
    )
    .agg(
        F.avg("PricePerltem").alias("avg_price"),
        F.sum("NumberOfItemPurchased").alias("total_quantity"),
        F.sum(F.col("PricePerltem") * F.col("NumberOfItemPurchased")).alias("RevenueItem")
    )
)


def load_data_postgres(data, epoch_id):
    try:
        window_spec = Window.partitionBy("ItemName", "ItemColor").orderBy("window.start")

        df_with_columns = data.withColumn("ts", F.current_timestamp()) \
                            .withColumn("total", F.sum("RevenueItem").over(window_spec)) \
                            .drop("window") 

        df_with_columns.write.format("jdbc").mode("append") \
            .option("url", postgres_url) \
            .option("dbtable", 'Avg_Retail_Data') \
            .option("user", postgres_user) \
            .option("password", postgres_password) \
            .save()
    except Exception as e:
        print(f"Error writing to PostgreSQL: {e}")

# Write the result to the console
# query = windowed_retail.writeStream.outputMode("complete").format("console").trigger(processingTime="10 seconds").start()

# query.awaitTermination()


# Write to PostgreSQL using foreachBatch
query = windowed_retail.writeStream.outputMode("update").foreachBatch(load_data_postgres).trigger(processingTime="10 seconds").start()

query.awaitTermination()

