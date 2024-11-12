import pyspark

spark = pyspark.sql.SparkSession.builder.appName("test_write").getOrCreate()

# generate data
df = spark.createDataFrame([
    ("user1", "Alice", 25, b"test photo data"),
    ("user2", "Bob", 30, b"test photo data"),
    ("user3", "Charlie", 35, b"test photo data")
], ["key", "name", "age", "photo"])

df.write.format("wds").mode("overwrite").save("xxx")