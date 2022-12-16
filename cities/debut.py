from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("helloworld").getOrCreate()

df = spark.read.csv("hdfs://localhost:9000/data/raw/cities/v1/csv/laposte_hexasmal.csv", header=True, sep=";")
df.write.mode("overwrite").parquet('/data/experiment/cities/v1/parquet')

spark.stop()
