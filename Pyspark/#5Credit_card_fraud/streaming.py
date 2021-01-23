from pyspark.sql import SparkSession 
from pyspark.sql.functions import split, col
from pyspark.ml import PipelineModel
from pyspark.sql.types import *

spark = SparkSession\
        .builder\
        .appName("TFM")\
        .getOrCreate()

lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", "credit_card_topic")\
        .load()\
        .select(col("value").cast("STRING")).alias("csv")\
        .select("csv.*")
df=lines \
      .selectExpr("split(value,',')[0] as Time" \
                 ,"split(value,',')[1] as V1" \
                 ,"split(value,',')[2] as V2" \
                 ,"split(value,',')[3] as V3" \
                 ,"split(value,',')[4] as V4" \
                 ,"split(value,',')[5] as V5" \
                 ,"split(value,',')[6] as V6" \
                 ,"split(value,',')[7] as V7" \
                 ,"split(value,',')[8] as V8" \
                 ,"split(value,',')[9] as V9" \
                 ,"split(value,',')[10] as V10" \
                 ,"split(value,',')[11] as V11" \
                 ,"split(value,',')[12] as V12" \
                 ,"split(value,',')[13] as V13" \
                 ,"split(value,',')[14] as V14" \
                 ,"split(value,',')[15] as V15" \
                 ,"split(value,',')[16] as V16" \
                 ,"split(value,',')[17] as V17" \
                 ,"split(value,',')[18] as V18" \
                 ,"split(value,',')[19] as V19" \
                 ,"split(value,',')[20] as V20" \
                 ,"split(value,',')[21] as V21" \
                 ,"split(value,',')[22] as V22" \
                 ,"split(value,',')[23] as V23" \
                 ,"split(value,',')[24] as V24" \
                 ,"split(value,',')[25] as V25" \
                 ,"split(value,',')[26] as V26" \
                 ,"split(value,',')[27] as V27" \
                 ,"split(value,',')[28] as V28" \
                 ,"split(value,',')[29] as Amount") 

df_1 = df.select([col(c).cast("float") for c in df.columns])

cvModelLoaded = PipelineModel.read().load('credit-model')

df_test_pred = cvModelLoaded.transform(df_1)
result = df_test_pred.drop('features', 'rawPrediction', 'probability')

query = result.writeStream\
        .format('parquet')\
        .option("path","output/hive/")\
        .option("checkpointLocation","output/checkpoint/")\
        .start()

query.awaitTermination()

