from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType
import datetime

spark = SparkSession.builder.appName("gogin_spark").getOrCreate()
schema = StructType() \
    .add("product_category_name", StringType()) \
    .add("product_category_name_english", StringType())

#читаем csv файлы в стриме
raw_files = spark \
    .readStream \
    .format("csv") \
    .schema(schema) \
    .options(path="input_csv_for_stream", header=True) \
    .load()

# разово проставляем время загрузки
load_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

#ВСЕГДА ПИЩЕМ И ОДНУ ДИРЕКТОРИЮ
def file_sink(df, freq):
    return df.writeStream.format("parquet") \
        .trigger(processingTime='%s seconds' % freq ) \
        .option("path","my_submit_parquet_files/p_date=" + str(load_time)) \
        .option("checkpointLocation", "checkpionts/my_parquet_checkpoint") \
        .start()

timed_files = raw_files.withColumn("p_date", F.lit("load_time"))

#запускаем стрим всегда в одну директорию
stream = file_sink(timed_files,10)

#will always spark.stop() at the end

#СТРИМ ТУТ ЖЕ ЗАКОНЧИТСЯ ПОТОМУ ЧТО В КОНЦЕ SPARK.STOP()