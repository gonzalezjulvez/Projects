import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegressionModel


spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

schema = StructType([StructField("Time",DoubleType(),True),StructField("V1",DoubleType(),True),StructField("V2",DoubleType(),True),StructField("V3",DoubleType(),True),StructField("V4",DoubleType(),True),StructField("V5",DoubleType(),True),StructField("V6",DoubleType(),True),StructField("V7",DoubleType(),True),StructField("features",DoubleType(),True),StructField("Amount",DoubleType(),True),StructField("Class",IntegerType(),True)])


lines = spark \
    .readStream \
    .schema(schema)\
    .csv("prueba", header=True)
model = LogisticRegressionModel.load("modelLR")

predictions = model.transform(lines)
# Split the lines into words

y_pred = predictions.select('Class')

# Generate running word count

query = y_pred \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()