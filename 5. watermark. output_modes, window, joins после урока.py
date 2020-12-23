#export SPARK_KAFKA_VERSION=0.10
# /spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 --driver-memory 512m --master local[1]
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

value_orders = raw_orders \
    .select(F.from_json(F.col("value").cast("String"), schema).alias("value"), "offset")

#добавляем колонку со временем получения сообщения
extended_orders = value_orders.select("value.order_id", "value.order_status", "value.order_purchase_timestamp" ) \
                    .withColumn("order_receive_time", F.current_timestamp())


extended_orders.printSchema()

#чекпоинт нужен для переноса кэша из оперативки в hdfs
def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .option("checkpointLocation", "checkpoints/duplicates_console_chk") \
        .options(truncate=False) \
        .start()

stream = console_output(extended_orders , 2)
stream.stop()

##WATERMARK должна очищать чекпоинт, но не гарантирует удаление точно вовремя. Возможно, удалит позже, но не раньше
#важно: она не для бизнес-логики, а для очистки чекпоинта!
waterwarked_orders = extended_orders.withWatermark("order_receive_time", "1 day")
waterwarked_orders.printSchema()

#получаем дедупликацию на каждый "тик"
#бизнес-логика вымышленная для наглядности примера: 1 заказ с уникальным статусом (хотя реальная ситуация -- уникальный order_id + order_status)

deduplicated_orders = waterwarked_orders.drop_duplicates(["order_status", "order_receive_time"])

stream = console_output(deduplicated_orders , 20)
stream.stop()




#WINDOW - получаем дедупликацию в промежуток времени
#создаем временное окно, объединяющее несколько микро-батчей (в зависимости от размера окна конечно же)
windowed_orders = extended_orders.withColumn("window_time",F.window(F.col("order_receive_time"),"2 minute"))
windowed_orders.printSchema()

stream = console_output(windowed_orders , 20)
stream.stop()
#устанавливаем вотермарк для очистки чекпоинта
waterwarked_windowed_orders = windowed_orders.withWatermark("window_time", "1 minute")
#удаляем дубли в каждом окне
deduplicated_windowed_orders = waterwarked_windowed_orders.drop_duplicates(["order_status", "window_time"])

stream = console_output(deduplicated_windowed_orders , 10)
stream.stop()


#SLIDING WINDOW - еще больше окон
#создаем временное окно
sliding_orders = extended_orders.withColumn("sliding_time",F.window(F.col("order_receive_time"),"1 minute","30 seconds"))
sliding_orders.printSchema()

stream = console_output(sliding_orders , 20)
stream.stop()
#устанавливаем вотермарк для очистки чекпоинта
waterwarked_sliding_orders = sliding_orders.withWatermark("sliding_time", "2 minute")
#удаляем дубли в каждом окне
deduplicated_sliding_orders = waterwarked_sliding_orders.drop_duplicates(["order_status", "sliding_time"])

stream = console_output(deduplicated_sliding_orders , 20)
stream.stop()



#OUTPUT MODES - считаем суммы
def console_output(df, freq, out_mode):
    return df.writeStream.format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=False) \
        .option("checkpointLocation", "checkpoints/my_watermark_console_chk2") \
        .outputMode(out_mode) \
        .start()

count_orders = waterwarked_windowed_orders.groupBy("window_time").count()
#пишем только обновляющиеся записи
stream = console_output(count_orders , 20, "update")
stream.stop()

#пишем все  записи
stream = console_output(count_orders , 20, "complete")
stream.stop()

#пишем все записи только один раз
stream = console_output(count_orders , 20, "append") #один раз в конце вотермарки
stream.stop()

sliding_orders = waterwarked_sliding_orders.groupBy("sliding_time").count()
#наблюдаем за суммами в плавающем окне
stream = console_output(sliding_orders , 20, "update")
stream.stop()


##JOINS

#stream-hdfs join
static_df = spark.table("spark_streaming_course.order_items")
static_joined = waterwarked_orders.join(static_df, "order_id", "inner" )
static_joined.isStreaming
static_joined.printSchema  #колонка order_id только одна

selected_static_joined = static_joined.select("order_id", "order_status", "order_purchase_timestamp", "order_receive_time", "order_item_id", "product_id")

stream = console_output(selected_static_joined , 1, "update")
stream.stop()

#stream-stream join
#stream №2
raw_orders_items = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "order_items"). \
    option("startingOffsets", "earliest"). \
    load()

##разбираем value
schema_items = StructType() \
    .add("order_id", StringType()) \
    .add("order_item_id", StringType()) \
    .add("product_id", StringType()) \
    .add("seller_id", StringType()) \
    .add("shipping_limit_date", StringType()) \
    .add("price", StringType()) \
    .add("freight_value", StringType())

extended_orders_items = raw_orders_items \
    .select(F.from_json(F.col("value").cast("String"), schema_items).alias("value")) \
    .select("value.*") \
    .withColumn("order_items_receive_time", F.current_timestamp()) \
    .withColumn("window_time",F.window(F.col("order_items_receive_time"),"10 minute"))

extended_orders_items.printSchema()


windowed_orders = extended_orders.withColumn("window_time",F.window(F.col("order_receive_time"),"10 minute"))
waterwarked_windowed_orders = windowed_orders.withWatermark("window_time", "10 minute")


streams_joined = waterwarked_windowed_orders \
    .join(extended_orders_items, ["order_id", "window_time"] , "inner") \
    .select("order_id", "order_item_id", "product_id", "window_time")

stream = console_output(streams_joined , 20, "update") #не сработает для inner
stream = console_output(streams_joined , 20, "append")
stream.stop()

streams_joined = waterwarked_windowed_orders \
    .join(extended_orders_items, ["order_id", "window_time"] , "left") \
    .select("order_id", "order_item_id", "product_id", "window_time")

stream = console_output(streams_joined , 20, "update")
stream.stop()

