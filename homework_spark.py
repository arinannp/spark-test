from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark = SparkSession.builder.appName('SparkHomework').getOrCreate()

df = spark.read.format("json").option("multiline","true").json("data/example2.json")
dfRenamed = df.withColumnRenamed("value", "hexcolor")
dfSorted = dfRenamed.sort(length(col("color")), ascending=True)

dfSorted.show(truncate=False)
