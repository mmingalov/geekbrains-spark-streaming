from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType


spark = SparkSession.builder.appName("gogin_spark").getOrCreate()


#функция, чтобы выводить на консоль вместо show()
def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=False) \
        .start()


######FILE SOURCE
##не сработает
raw_files = spark \
    .readStream \
    .format("csv") \
    .options(path="input_csv_for_stream") \
    .load()

#требует схему:
schema = StructType() \
    .add("product_category_name", StringType()) \
    .add("product_category_name_english", StringType())

#все разом
raw_files = spark \
    .readStream \
    .format("csv") \
    .schema(schema) \
    .options(path="input_csv_for_stream", header=True) \
    .load()

out = console_output(raw_files, 5)
out.stop()

#по одному
raw_files = spark \
    .readStream \
    .format("csv") \
    .schema(schema) \
    .options(path="input_csv_for_stream",
             header=True,
             maxFilesPerTrigger=1) \
    .load()

out = console_output(raw_files, 5)
out.stop()

#так же добавляем свою колонку
extra_files = raw_files \
    .withColumn("spanish_length", F.length(F.col("product_category_name"))) \
    .withColumn("english_length", F.length(F.col("product_category_name_english"))) \
    .filter(F.col("spanish_length")==F.col("english_length"))

out = console_output(extra_files, 5)
out.stop()

