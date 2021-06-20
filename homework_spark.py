from pyspark.sql import SparkSession
from pyspark.sql.functions import *



if __file__ == "__main__":
    
    spark = SparkSession.builder \
        .builder \
        .appName("HomeworkSparkCode") \
        .getOrCreate()

    df = spark.read.format("json").option("multiline","true").json("data/example2.json")
    dfRenamed = df.withColumnRenamed("value", "hexcolor")
    dfSorted = dfRenamed.sort(length(col("color")), ascending=True)
    
    dfSorted.show(truncate=False)