import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

print(f"Spark version: {spark.version}")

df = spark.range(10)
df.show()

print("Spark version:", spark.version) 
print("Spark UI:", spark.sparkContext.uiWebUrl)

input("UI is up. Press Enter to stop...")

spark.stop()