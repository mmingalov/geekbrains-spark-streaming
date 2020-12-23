#export SPARK_KAFKA_VERSION=0.10
#/spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 --driver-memory 512m --driver-cores 1 --master local[1]

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("gogin_spark").getOrCreate()

def explain(self, extended=True):
    if extended:
        print(self._jdf.queryExecution().toString())
    else:
        print(self._jdf.queryExecution().simpleString())

cass_animals_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="animals", keyspace="lesson7") \
    .load()

cass_animals_df.printSchema()
cass_animals_df.show()

cass_animals_df.write.parquet("my_parquet_from_cassandra")


cow_df = spark.sql("""select 11 as id, "Cow" as name, "Big" as size """)
cow_df.show()

cow_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="animals", keyspace="lesson7") \
    .mode("append") \
    .save()

#айди такой же, имя другое
bull_df = spark.sql("""select 11 as id, "Bull" as name, "Big" as size """)
bull_df.show()

bull_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="animals", keyspace="lesson7") \
    .mode("append") \
    .save()

#выдаст предупреждение и попросит дополнительный параметр из за overwrite
bull_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="animals", keyspace="lesson7") \
    .mode("overwrite") \
    .save()


#теперь читаем большой большой датасет по ключу
cass_big_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="users_many", keyspace="keyspace1") \
    .load()

cass_big_df.show()
#выполняем запрос попадая в ключ
cass_big_df.filter(F.col("user_id")=="10").show()
#выполняем запрос не попадая в ключ
cass_big_df.filter(F.col("gender")=="1").show() #быстро из за предела в 20
cass_big_df.filter(F.col("gender")=="1").count() #медленно

#Наблюдаем на pushedFilter в PhysicalPlan
explain(cass_big_df.filter(F.col("user_id")=="10"))

explain(cass_big_df.filter(F.col("gender")=="10"))

#between не передается в pushdown
cass_big_df.createOrReplaceTempView("cass_df")
sql_select = spark.sql("""
select * 
from cass_df
where user_id between 1999 and 2000
""")
#проверяем, что user id не попал в pushedFilter
explain(sql_select)
sql_select.show() #медленно

#in передается в pushdown
sql_select = spark.sql("""
select * 
from cass_df
where user_id in (3884632855,3562535987)
""")

#проверяем, что user id попал в pushedFilter
explain(sql_select)
sql_select.show() #быстро