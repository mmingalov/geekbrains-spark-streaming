from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType


spark = SparkSession.builder.appName("gogin_spark").getOrCreate()


######RATE SOURCE
raw_rate = spark \
    .readStream \
    .format("rate") \
    .load()

raw_rate.printSchema()

raw_rate.isStreaming

raw_rate.show()      #не сработает


stream = raw_rate.writeStream \
    .format("console") \
    .start()   #побежит быстро

stream.stop()

#запускаем медленно
stream = raw_rate \
    .writeStream \
    .trigger(processingTime='30 seconds') \
    .format("console") \
    .options(truncate=False) \
    .start()


#проверяем параметры
stream.explain()
stream.isActive
stream.lastProgress
stream.status

stream.stop()


#функция, чтобы выводить на консоль, вместо show()
def console_output(df, freq):
    return df.writeStream \
        .format("console") \
        .trigger(processingTime='%s seconds' % freq ) \
        .options(truncate=False) \
        .start()

out = console_output(raw_rate, 5)
out.stop()



#добавляем собственный фильтр
filtered_rate = raw_rate \
    .filter( F.col("value") % F.lit("2") == 0 )

out = console_output(filtered_rate, 5)
out.stop()

#добавляем собственные колонки
extra_rate = filtered_rate \
    .withColumn("my_value",
                F.when((F.col("value") % F.lit(10) == 0), F.lit("jubilee"))
                    .otherwise(F.lit("not yet")))

out = console_output(extra_rate, 5)
out.stop()

#если потеряем стрим из переменной, сможем остановить все наши стримы, получив их из спарк окружения
def killAll():
    for active_stream in spark.streams.active:
        print("Stopping %s by killAll" % active_stream)
        active_stream.stop()





