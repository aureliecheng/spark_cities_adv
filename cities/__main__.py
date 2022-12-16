from pyspark.sql import SparkSession
from cities.main import main

if __name__ == "__main__":
  spark = SparkSession.builder.appName("cities").getOrCreate()
  main(spark)
  spark.stop()
  