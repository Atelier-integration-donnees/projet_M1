from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CheckSilver").master("local[*]").getOrCreate()
df = spark.read.parquet("data/silver")
df.printSchema()
print("Total rows:", df.count())
spark.stop()
