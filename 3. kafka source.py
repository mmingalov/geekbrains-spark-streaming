#export SPARK_KAFKA_VERSION=0.10
# pyspark2 --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType

spark = SparkSession.builder.appName("gogin_spark").getOrCreate()

kafka_brokers = "bigdataanalytics-worker-0.novalocal:6667"

#функция, чтобы выводить на консоль вместо show()
def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=True) \
        .start()

#читаем без стрима
raw_orders = spark.read. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "orders_json"). \
    option("startingOffsets", "earliest"). \
    load()

raw_orders.show()
raw_orders.show(1,False)

#прочитали до 20го оффсета
raw_orders = spark.read. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "orders_json"). \
    option("startingOffsets", "earliest"). \
    option("endingOffsets", """{"orders_json":{"0":20}}"""). \
    load()

raw_orders.show(100)

# прочитали в стриме ВСЁ
raw_orders = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "orders_json"). \
    option("startingOffsets", "earliest"). \
    load()

out = console_output(raw_orders, 5)
out.stop()

# прочитали потихоньку
raw_orders = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "orders_json"). \
    option("startingOffsets", "earliest"). \
    option("maxOffsetsPerTrigger", "5"). \
    load()

out = console_output(raw_orders, 5)
out.stop()


# прочитали один раз с конца
raw_orders = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "orders_json"). \
    option("maxOffsetsPerTrigger", "5"). \
    option("startingOffsets", "latest"). \
    load()

out = console_output(raw_orders, 5)
out.stop()


# прочитали с 10го оффсета
raw_orders = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "orders_json"). \
    option("startingOffsets", """{"orders_json":{"0":10}}"""). \
    option("maxOffsetsPerTrigger", "5"). \
    load()

out = console_output(raw_orders, 5)
out.stop()


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


value_orders = raw_orders \
    .select(F.from_json(F.col("value").cast("String"), schema).alias("value"), "offset")

value_orders.printSchema()

parsed_orders = value_orders.select("value.*", "offset")

parsed_orders.printSchema()

out = console_output(parsed_orders, 30)
out.stop()

#добавляем чекпоинт
def console_output_checkpointed(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq) \
        .option("truncate",False) \
        .option("checkpointLocation", "orders_console_checkpoint") \
        .start()

out = console_output_checkpointed(parsed_orders, 5)
out.stop()
