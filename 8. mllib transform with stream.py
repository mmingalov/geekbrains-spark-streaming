#export SPARK_KAFKA_VERSION=0.10
#/spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 --driver-memory 512m --driver-cores 1 --master local[1]

from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import OneHotEncoderEstimator, VectorAssembler, CountVectorizer, StringIndexer, IndexToString

spark = SparkSession.builder.appName("gogin_spark").getOrCreate()

kafka_brokers = "bigdataanalytics-worker-1.novalocal:6667"

#читаем кафку по одной записи, но можем и по 1000 за раз
sales = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "sales_unknown"). \
    option("startingOffsets", "earliest"). \
    option("maxOffsetsPerTrigger", "1"). \
    load()

schema = StructType() \
    .add("order_id", IntegerType()) \
    .add("user_id", IntegerType()) \
    .add("items_count", IntegerType()) \
    .add("price", IntegerType()) \
    .add("order_date", StringType())

value_sales = sales.select(F.from_json(F.col("value").cast("String"), schema).alias("value"), "offset")

sales_flat = value_sales.select(F.col("value.*"), "offset")

def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=True) \
        .start()

s = console_output(sales_flat, 5)
s.stop()

###############
#подготавливаем DataFrame для запросов к касандре с историческими данными
cassandra_features_raw = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="users_unknown", keyspace="keyspace1" ) \
    .load()

cassandra_features_raw.show()

cassandra_features_selected = cassandra_features_raw.selectExpr("user_id", "age", "gender", "c",
                                                                "s1", "ma1", "case when mi1 is null then 9999999 else mi1 end as mi1",
                                                                "s2", "ma2", "case when mi2 is null then 9999999 else mi2 end as mi2")

cassandra_features_selected.show()


#подгружаем ML из HDFS
pipeline_model = PipelineModel.load("my_LR_model8")

##########
#вся логика в этом foreachBatch
def writer_logic(df, epoch_id):
    df.persist()
    print("---------I've got new batch--------")
    print("This is what I've got from Kafka:")
    df.show()
    features_from_kafka = df.groupBy("user_id").agg(F.lit("").alias("age"), F.lit("").alias("gender"), F.count("*").alias("c"), \
                             F.sum("items_count").alias("s1"), F.max("items_count").alias("ma1"), F.min("items_count").alias("mi1"), \
                             F.sum("price").alias("s2"), F.max("price").alias("ma2"), F.min("price").alias("mi2"))
    print("Here is the sums from Kafka:")
    features_from_kafka.show()
    users_list_df = features_from_kafka.select("user_id").distinct()
    #превращаем DataFrame(Row) в Array(Row)
    users_list_rows = users_list_df.collect()
    #превращаем Array(Row) в Array(String)
    users_list = map( lambda x: str(x.__getattr__("user_id")) , users_list_rows )
    where_string = " user_id = " + " or user_id = ".join(users_list)
    print("I'm gonna select this from Cassandra:")
    print(where_string)
    features_from_cassandra = cassandra_features_selected.where(where_string).na.fill(0)
    features_from_cassandra.persist()
    print("Here is what I've got from Cassandra:")
    features_from_cassandra.show()
    #объединяем микробатч из кафки и микробатч касандры
    cassandra_kafka_union = features_from_kafka.union(features_from_cassandra)
    cassandra_kafka_aggregation = cassandra_kafka_union.groupBy("user_id"). \
        agg(F.max("age").alias("age"), F.max("gender").alias("gender"), F.sum("c").alias("c"), \
            F.sum("s1").alias("s1"), F.max("ma1").alias("ma1"), F.min("mi1").alias("mi1"), \
            F.sum("s2").alias("s2"), F.max("ma2").alias("ma2"), F.min("mi2").alias("mi2"))
    print("Here is how I aggregated Cassandra and Kafka:")
    cassandra_kafka_aggregation.show()
    predict = pipeline_model.transform(cassandra_kafka_aggregation)
    predict_short = predict.select("user_id", "age", "gender", "c", "s1", "ma1", "mi1",  "s2", "ma2", "mi2", F.col("category").alias("segment"))
    print("Here is what I've got after model transformation:")
    predict_short.show()
    #обновляем исторический агрегат в касандре
    predict_short.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="users_unknown", keyspace="keyspace1") \
        .mode("append") \
        .save()
    features_from_cassandra.unpersist()
    print("I saved the prediction and aggregation in Cassandra. Continue...")
    df.unpersist()

#использовал функцию для тестирования, может кому пригодится
#array = map(lambda x: x.__getattr__("user_id"),cassandra_features_selected.select("user_id").distinct().collect() )

#связываем источник Кафки и foreachBatch функцию
stream = sales_flat \
    .writeStream \
    .trigger(processingTime='100 seconds') \
    .foreachBatch(writer_logic) \
    .option("checkpointLocation", "checkpoints/sales_unknown_checkpoint")

#поехали
s = stream.start()

s.stop()
