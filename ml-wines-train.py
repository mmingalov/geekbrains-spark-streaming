#export SPARK_KAFKA_VERSION=0.10
#/spark2.4/bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.spark:spark-cassandra-connector_2.11:2.4.2 --driver-memory 512m --driver-cores 1 --master local[1]

from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import OneHotEncoderEstimator, VectorAssembler, CountVectorizer, StringIndexer, IndexToString

spark = SparkSession.builder.appName("mmingalov_spark").getOrCreate() #не нужна для консоли, только для IDE
train = spark.read\
    .option("header", True)\
    .csv("winequality-red.csv", sep=',')\
    .cache()

train = spark.read\
    .option("header", True)\
    .csv("winequality-red.csv", sep=',')\
    .withColumn("fixed acidity", col("fixed acidity").cast('float'))\
    .withColumn("volatile acidity", col("volatile acidity").cast('float'))\
    .withColumn("citric acid", col("citric acid").cast('float'))\
    .withColumn("residual sugar", col("residual sugar").cast('float'))\
    .withColumn("chlorides", col("chlorides").cast('float'))\
    .withColumn("free sulfur dioxide", col("free sulfur dioxide").cast('float'))\
    .withColumn("total sulfur dioxide", col("total sulfur dioxide").cast('float'))\
    .withColumn("density", col("density").cast('float'))\
    .withColumn("pH", col("pH").cast('float'))\
    .withColumn("sulphates", col("sulphates").cast('float'))\
    .withColumn("alcohol", col("alcohol").cast('float'))\
    .withColumn("quality", col("quality").cast('float'))\
    .cache()
    # .withColumn("volatile acidity", F.expr("CAST([volatile acidity] as FLOAT)"))\
    # .withColumn("citric acid", F.expr("CAST([citric acid] as FLOAT)"))\
    # .withColumn("residual sugar", F.expr("CAST([residual sugar] as FLOAT)"))\
    # .withColumn("chlorides", F.expr("CAST(chlorides as FLOAT)"))\
    # .withColumn("free sulfur dioxide", F.expr("CAST([free sulfur dioxide] as FLOAT)"))\
    # .withColumn("total sulfur dioxide", F.expr("CAST([total sulfur dioxide] as FLOAT)"))\
    # .withColumn("density", F.expr("CAST(density as FLOAT)"))\
    # .withColumn("pH", F.expr("CAST(pH as FLOAT)"))\
    # .withColumn("sulphates", F.expr("CAST(sulphates as FLOAT)"))\
    # .withColumn("alcohol", F.expr("CAST(alcohol as FLOAT)"))\
    # .withColumn("quality", F.expr("CAST(quality as FLOAT)"))\


train.show(5, False)
train.printSchema()

stages = []
label_stringIdx = StringIndexer(inputCol = 'quality', outputCol = 'label').setHandleInvalid("keep")
stages += [label_stringIdx]

assemblerInputs = ['fixed acidity','volatile acidity','citric acid','residual sugar','chlorides','free sulfur dioxide','total sulfur dioxide','density','pH','sulphates','alcohol']
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features").setHandleInvalid("keep")
stages += [assembler]

lr = LinearRegression(featuresCol = 'features', labelCol = 'label', maxIter=10)
stages += [lr]

label_stringIdx_fit = label_stringIdx.fit(train)
indexToStringEstimator = IndexToString().setInputCol("prediction").setOutputCol("predicted_points").setLabels(label_stringIdx_fit.labels)

stages +=[indexToStringEstimator]

pipeline = Pipeline().setStages(stages)
pipelineModel = pipeline.fit(train)

#сохраняем модель на HDFS
pipelineModel.write().overwrite().save("wines_LR_model8_mmingalov")

###для наглядности
pipelineModel.transform(train).select("quality", "predicted_points").show(100)

#rmse-метрика
from pyspark.ml.evaluation import RegressionEvaluator
regressionEvaluator = RegressionEvaluator(
    predictionCol="predicted_points",
    labelCol="quality",
    metricName="rmse")

prediction = pipelineModel.transform(train).select(F.col("quality").cast("Float"),
                                                   F.col("predicted_points").cast("Float"))
rmse = regressionEvaluator.evaluate(prediction)
print("RMSE is " + str(rmse))

pipelineModel.transform(train).show()

