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
    option("maxOffsetsPerTrigger", "20"). \
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

extended_orders = parsed_orders \
    .withColumn("my_extra_column", F.round( F.rand() * 100 ) ) \
    .withColumn("my_current_time", F.current_timestamp())



#FOREACH BATCH SINK
def foreach_batch_sink(df, freq):
    return  df \
        .writeStream \
        .foreachBatch(foreach_batch_function) \
        .trigger(processingTime='%s seconds' % freq ) \
        .start()

def foreach_batch_function(df, epoch_id):
    print("starting epoch " + str(epoch_id) )
    df.persist()
    df.filter(F.col("order_status")!="delivered"). \
        select("order_id", "order_status"). \
        withColumn("reason", F.lit("too slow")). \
        show(truncate=False)
    df.filter(F.col("order_status")=="delivered"). \
        select("order_status", "order_delivered_customer_date"). \
        withColumn("delivery_time", F.lit("very fast delivery")). \
        show(truncate=False)
    df.unpersist()
    print("finishing epoch " + str(epoch_id))


stream = foreach_batch_sink(extended_orders,20)
stream.stop()

#with persist
def foreach_batch_function(df, epoch_id):
    print("I DO START THE BATCH")
    df.persist() #сохрянем в память для многократного фильтра
    #получаем DF хороших доставок
    good_job = df.filter(F.col("order_status")=="delivered"). \
        select("order_status", "order_delivered_customer_date"). \
        withColumn("delivery_time", F.lit("very fast delivery"))
    #получаем DF плохих доставок
    bad_job = df.filter(F.col("order_status")!="delivered"). \
        select("order_id", "order_status"). \
        withColumn("reason", F.lit("too slow"))
    #сохраняем DF плохих доставок для многократных action
    bad_job.persist()
    print("start writing these bad jobs:")
    bad_job.show()
    bad_job.write.mode("append").parquet("my_bad_job") #записываем плохие доставки в папку на hdfs
    print("Bad jobs written: " +  str(bad_job.count()))
    bad_job.unpersist() #удаляем из памяти плохие доставки
    good_job.persist() #сохраняем DF хороших доставок для многократных action
    print("start writing these good jobs:")
    good_job.show()
    df.unpersist() #удаляем из памяти полный набор
    print("start writing these good jobs:")
    good_job.write.mode("append").parquet("my_good_job") #записываем хорошие доставки в папку на hdfs
    print("Good jobs: written: " + str(good_job.count()))
    good_job.unpersist() #удаляем из памяти хорошие доставки
    print("I FINISHED THE BATCH")


stream = foreach_batch_sink(extended_orders,30)
stream.stop()


