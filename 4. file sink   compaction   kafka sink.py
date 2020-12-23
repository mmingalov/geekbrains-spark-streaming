#export SPARK_KAFKA_VERSION=0.10
#/spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --driver-memory 512m --num-executors 1 --executor-memory 512m --master local[1]
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

spark = SparkSession.builder.appName("gogin_spark").getOrCreate()

kafka_brokers = "bigdataanalytics-worker-0.novalocal:6667"

raw_orders = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "orders_json"). \
    option("maxOffsetsPerTrigger", "5"). \
    option("startingOffsets", "earliest"). \
    load()

##разбираем value
schema = StructType() \
    .add("order_id", StringType()) \
    .add("customer_id", StringType()) \
    .add("order_status", StringType()) \
    .add("order_purchase_timestamp", StringType()) \
    .add("order_approved_at", StringType()) \
    .add("order_delivered_carrier_date", StringType()) \
    .add("order_delivered_customer_date", StringType()) \
    .add("order_estimated_delivery_date", StringType())

parsed_orders = raw_orders \
    .select(F.from_json(F.col("value").cast("String"), schema).alias("value"), "offset") \
    .select("value.*", "offset")

def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=False) \
        .start()

out = console_output(parsed_orders, 5)
out.stop()

##MEMORY SINK
def memory_sink(df, freq):
    return df.writeStream.format("memory") \
        .queryName("my_memory_sink_table") \
        .trigger(processingTime='%s seconds' % freq ) \
        .start()

stream = memory_sink(parsed_orders,5)
spark.sql("select * from my_memory_sink_table").show()
spark.sql("""select *, current_timestamp() as my_extra_column from my_memory_sink_table order_status = "delivered" """).show()


###FILE SINK (only with checkpoint)
def file_sink(df, freq):
    return df.writeStream.format("parquet") \
        .trigger(processingTime='%s seconds' % freq ) \
        .option("path","my_parquet_sink") \
        .option("checkpointLocation", "my_parquet_checkpoint") \
        .start()

stream = file_sink(parsed_orders,1)  #для демонстрации чекпоинта поменять maxOffsetsPerTrigger
stream.stop()

##COMPACTION нужно дописать на партиции оverwrite
def compact_directory(path):
    df_to_compact = spark.read.parquet(path + "/*.parquet")
    df_to_compact.persist()
    df_to_compact.count()  #для активации персиста
    df_to_compact.repartition(1).write.mode("overwrite").parquet(path)
    df_to_compact.unpersist()

compact_directory("my_parquet_sink")


#KAFKA SINK
def kafka_sink(df, freq):
    return df.selectExpr("CAST(null AS STRING) as key", "CAST(struct(*) AS STRING) as value") \
        .writeStream \
        .format("kafka") \
        .trigger(processingTime='%s seconds' % freq ) \
        .option("topic", "kafka_sink") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("checkpointLocation", "my_kafka_checkpoint") \
        .start()

stream = kafka_sink(parsed_orders,5)
stream.stop()


def kafka_sink_json(df, freq):
    return df.selectExpr("CAST(null AS STRING) as key", "CAST(to_json(struct(*)) AS STRING) as value") \
        .writeStream \
        .format("kafka") \
        .trigger(processingTime='%s seconds' % freq ) \
        .option("topic", "kafka_sink") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("checkpointLocation", "my_kafka_checkpoint") \
        .start()

stream = kafka_sink_json(parsed_orders,5)
stream.stop()
