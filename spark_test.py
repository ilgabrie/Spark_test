import pyspark
from pyspark.sql import *

spark = SparkSession.builder.appName("SparkTest").getOrCreate()

df1 = (spark.read
      .format("csv")
      .option("delimiter", ",")
      .option("header", True)
      .option("inferSchema", True)
      .load("maprfs:///testdata/nyc_data/raw/green_taxi/*.csv"))
df2 = (spark.read
      .format("csv")
      .option("delimiter", ",")
      .option("header", True)
      .option("inferSchema", True)
      .load("maprfs:///testdata/nyc_data/raw/yellow_taxi/*.csv"))
df1.createOrReplaceTempView("green_taxi_table")
df2.createOrReplaceTempView("yellow_taxi_table")
df_count = spark.sql("select sum(cnt) as total_count from (select count(*) as cnt from green_taxi_table union all select count(*) as cnt from yellow_taxi_table)")
(df_count.write
  .format("csv")
  .option("header", "true")
  .mode("overwrite")
  .save("maprfs:///testdata/output")
)
