#export SPARK_KAFKA_VERSION=0.10
#/spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 --driver-memory 512m --driver-cores 1 --master local[1]
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, TimestampType
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("mmingalov_spark").getOrCreate() #не нужна для консоли, только для IDE
#kafka_brokers = "bigdataanalytics-worker-1.novalocal:6667"
kafka_brokers = "bigdataanalytics-worker-0.novalocal:6667"

def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=True) \
        .start()

#набор features 1 для Kafka
schema = StructType() \
    .add("id", IntegerType()) \
    .add("fixed acidity", FloatType()) \
    .add("volatile acidity", FloatType()) \
    .add("citric acid", FloatType())
    # .add("residual sugar", FloatType()) \
    # .add("chlorides", FloatType()) \
    # .add("free sulfur dioxide", FloatType()) \
    # .add("total sulfur dioxide", FloatType()) \
    # .add("density", FloatType()) \
    # .add("pH", FloatType()) \
    # .add("sulphates", FloatType()) \
    # .add("alcohol", FloatType())


# csv - чтение из файла, sample
wines_acid_info = spark \
    .readStream\
    .format("csv")\
    .schema(schema)\
    .options(path="input_csv_for_stream_lesson8",
             sep=";",
             header=True,
             maxFilesPerTrigger=1)\
    .load()

out = console_output(wines_acid_info, 5)
out.stop()

# запись в Кафку - sink, заранее создаем топик командой:
# /usr/hdp/3.1.4.0-315/kafka/bin/kafka-topics.sh --create --topic lesson8_mmingalov --zookeeper bigdataanalytics-worker-0.novalocal:2181 --partitions 1 --replication-factor 2 --config retention.ms=-1
# не забываем удалять чекпоинт!
def kafka_sink(df, freq):
    return df.selectExpr("CAST(null AS STRING) as key", "CAST(struct(*) AS STRING) as value") \
        .writeStream \
        .format("kafka") \
        .trigger(processingTime='%s seconds' % freq ) \
        .option("topic", "lesson8_mmingalov") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("checkpointLocation", "checkquality/kafka_checkpoint") \
        .start()

stream = kafka_sink(wines_acid_info, 5)

#этот процесс надо оставить, чтобы ловить обновления
#stream.stop()

#читаем кафку по одной записи, но можем и по 1000 за раз
wines_acid_info_kafka = spark.readStream. \
    format("kafka"). \
    option("kafka.bootstrap.servers", kafka_brokers). \
    option("subscribe", "lesson8_mmingalov"). \
    option("startingOffsets", "earliest"). \
    option("maxOffsetsPerTrigger", "1"). \
    load()

k = console_output(wines_acid_info_kafka, 5)
k.stop()

# VALUE строкой. Принимает только cast("String"), другие форматы не дает задать
value_wines_acid_info = wines_acid_info_kafka.select(F.regexp_replace(F.col("value").cast("String"),
                                                                  "([\[\]])", "").alias("value"),
                                                                  "offset")

# для стрингов достаточно такой конструкции
#parsed_wines_info = value_wines_info.selectExpr("split(value, ',')[0] as id",
#                                                      "split(value, ',')[1] as age",
#                                                      "split(value, ',')[2] as years_of_experience",
#                                                      "split(value, ',')[3] as lesson_price",
#                                                      "offset")

# сразу переводим в числовой формат
parsed_wines_acid_info = value_wines_acid_info.selectExpr(
                                                "CAST(split(value, ',')[0] as INTEGER) as id",
                                                "CAST(split(value, ',')[1] as FLOAT) as fixed_acidity",
                                                "CAST(split(value, ',')[2] as FLOAT) as volatile_acidity",
                                                "CAST(split(value, ',')[3] as FLOAT) as citric_acid",
                                                "offset")

s = console_output(parsed_wines_acid_info, 5)
s.stop()

#подготавливаем DataFrame для запросов к кассандре с историческими данными
#нужны заранее keyspace и таблица того же размера, что и в кафке или файле-источнике
"""
/cassandra/bin/cqlsh 10.0.0.18 — запуск

создать схему
CREATE  KEYSPACE  lesson8_mmingalov
   WITH REPLICATION = {
      'class' : 'SimpleStrategy', 'replication_factor' : 1 } ;

use lesson8_mmingalov;

DROP TABLE test_wines_quality;
DROP TABLE test_wines_quality_predicted;

CREATE TABLE test_wines_quality
(Id int primary key, 
fixed_acidity float,
volatile_acidity float,
citric_acid float,
residual_sugar float,
chlorides float,
free_sulfur_dioxide float,
total_sulfur_dioxide float,
density float,
pH float,
sulphates float,
alcohol float
);

# большие буквы переводит в маленькие, тот же формат, что и фичи для записи
CREATE TABLE test_wines_quality_predicted
(Id int primary key, 
fixed_acidity float,
volatile_acidity float,
citric_acid float,
residual_sugar float,
chlorides float,
free_sulfur_dioxide float,
total_sulfur_dioxide float,
density float,
pH float,
sulphates float,
alcohol float,
quality float);
"""

# schema_other = StructType() \
#     .add("id", IntegerType()) \
#     .add("residual_sugar", FloatType()) \
#     .add("chlorides", FloatType()) \
#     .add("free sulfur dioxide", FloatType()) \
#     .add("total sulfur dioxide", FloatType()) \
#     .add("density", FloatType()) \
#     .add("pH", FloatType()) \
#     .add("alcohol", FloatType())
#
# #path="for_cassandra"
# wines_quality = spark.read\
#     .format("csv")\
#     .schema(schema_other)\
#     .options(path="input_csv_for_stream_lesson8",
#              sep=";",
#              header=True,
#              maxFilesPerTrigger=1)\
#     .load()

#прочитаем всю схему как она есть в CSV и возьмем потом только столбцы для поднабора 2
schema_ = StructType() \
    .add("id", IntegerType()) \
    .add("fixed acidity", FloatType()) \
    .add("volatile acidity", FloatType()) \
    .add("citric acid", FloatType()) \
    .add("residual sugar", FloatType()) \
    .add("chlorides", FloatType()) \
    .add("free sulfur dioxide", FloatType()) \
    .add("total sulfur dioxide", FloatType()) \
    .add("density", FloatType()) \
    .add("pH", FloatType()) \
    .add("sulphates", FloatType()) \
    .add("alcohol", FloatType())

#path="for_cassandra"
wines_ = spark.read\
    .format("csv")\
    .schema(schema_)\
    .options(path="input_csv_for_stream_lesson8",
             sep=";",
             header=True,
             maxFilesPerTrigger=1)\
    .load()
wines_.show(5, False)

#собственно поднабор 2
wines_quality = wines_.select(
    "id",
    F.col("residual sugar").alias("residual_sugar"),
    "chlorides",
    F.col("free sulfur dioxide").alias("free_sulfur_dioxide"),
    F.col("total sulfur dioxide").alias("total_sulfur_dioxide"),
    "density",
    "ph", #с маленькой буквы!!
    "sulphates",
    "alcohol"
)

wines_quality.show(5, False)

# положить "исторические" данные в Кассандру
wines_quality.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="test_wines_quality", keyspace="lesson8_mmingalov") \
    .mode("append") \
    .save()

# читаем из Кассандры
cassandra_features_raw = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="test_wines_quality", keyspace="lesson8_mmingalov" ) \
    .load()

cassandra_features_raw.show()

# без пробелов?!
cassandra_features_selected = cassandra_features_raw.select('id','fixed_acidity',
                                                                'volatile_acidity','citric_acid','residual_sugar',
                                                                'chlorides','free_sulfur_dioxide','total_sulfur_dioxide',
                                                                'density','pH','sulphates','alcohol')
cassandra_features_selected.show()

#подгружаем ML из HDFS
pipeline_model = PipelineModel.load("wines_LR_model8_mmingalov")

##########

#вся логика в этом foreachBatch
def writer_logic(df, epoch_id):
    df.persist()
    print("---------I've got new batch--------")
    print("This is what I've got from Kafka source:")
    df.show()
    features_from_kafka = df
    # features_from_kafka = df.groupBy("id") \
    #     .agg(F.lit(0.0).alias("fixed_acidity"),
    #          F.lit(0.0).alias("volatile_acidity"), \
    #          F.lit(0.0).alias("citric_acid"), \
    #          F.lit(0.0).alias("residual_sugar"), \
    #          F.lit(0.0).alias("chlorides"),
    #          F.lit(0.0).alias("free_sulfur_dioxide"), \
    #          F.lit(0.0).alias("total_sulfur_dioxide"), \
    #          F.lit(0.0).alias("density"), \
    #          F.lit(0.0).alias("ph"), \
    #          F.lit(0.0).alias("sulphates"), \
    #          F.lit(0.0).alias("alcohol"))
    print("Here is the sums from Kafka source:")
    features_from_kafka.show()

    tt_list_df = features_from_kafka.select("id").distinct()
    #превращаем DataFrame(Row) в Array(Row)
    wines_list_rows = tt_list_df.collect()

    #превращаем Array(Row) в Array(String)
    wines_list = map( lambda x: str(x.__getattr__("id")) , wines_list_rows)
    where_string = " id = " + " or id = ".join(wines_list)
    print("I'm gonna select this from Cassandra:")
    print(where_string)
    print("Here is what I've got from Cassandra:")
    cassandra_features_selected.where(where_string).show()
    features_from_cassandra = cassandra_features_selected.where(where_string).na.fill(0)
    features_from_cassandra.persist()
    print("I've replaced nulls with 0 from Cassandra:")
    features_from_cassandra.show()

    #объединяем микробатч из кафки и микробатч касандры
    # cassandra_file_union = features_from_kafka.union(features_from_cassandra)
    # cassandra_file_aggregation = cassandra_file_union.groupBy("id") \
    #     .agg(F.lit("fixed_acidity").alias("fixed_acidity"),
    #          F.lit("volatile_acidity").alias("volatile_acidity"), \
    #          F.lit("citric_acid").alias("citric_acid"), \
    #          F.lit("residual_sugar").alias("residual_sugar"), \
    #          F.lit("chlorides").alias("chlorides"),
    #          F.lit("free_sulfur_dioxide").alias("free_sulfur_dioxide"), \
    #          F.lit("total_sulfur_dioxide").alias("total_sulfur_dioxide"), \
    #          F.lit("density").alias("density"), \
    #          F.lit("ph").alias("ph"), \
    #          F.lit("sulphates").alias("sulphates"), \
    #          F.lit("alcohol").alias("alcohol"))
    # print("Here is how I aggregated Cassandra and file:")
    # cassandra_file_aggregation.show()

    # объединяем микробатч из кафки и микробатч касандры
    cassandra_file_joined = features_from_kafka.join(features_from_cassandra, "id")
    predict = pipeline_model.transform(cassandra_file_joined)
    print("I've got the prediction:")
    predict.show()
    predict_short = predict.select('id', 'fixed acidity', 'volatile acidity', 'citric acid',
                                   'residual sugar', 'chlorides', 'free sulfur dioxide',
                                   'total sulfur dioxide', 'density', 'ph', 'sulphates', 'alcohol',
                                   F.col('predicted_quality').cast(FloatType()).alias('quality'))
    print("Here is what I've got after model transformation:")
    predict_short.show()
    #обновляем исторический агрегат в касандре - записываем в другую таблицу
    predict_short.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="test_wines_quality_predicted", keyspace="lesson8_mmingalov") \
        .mode("append") \
        .save()
    features_from_cassandra.unpersist()
    print("I saved the prediction and aggregation in Cassandra. Continue...")
    df.unpersist()

#связываем источник и foreachBatch функцию, не забываем удалять чекпоинт
stream_foreachBatch = parsed_wines_acid_info \
    .writeStream \
    .trigger(processingTime='30 seconds') \
    .foreachBatch(writer_logic) \
    .option("checkpointLocation", "checkquality/test_wines_checkpoint")

#поехали
s = stream_foreachBatch.start()

s.stop()

def killAll():
    for active_stream in spark.streams.active:
        print("Stopping %s by killAll" % active_stream)
        active_stream.stop()

killAll()