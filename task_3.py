from pyspark import sql, SparkConf, SparkContext

conf = SparkConf().setAppName("task_1")
sc = SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)

df = sqlContext.read.csv("artists.csv")
df = df.withColumn("_c4", df["_c4"].cast(sql.types.IntegerType()))

print(df.agg({"_c4": "min"}).collect()[0][0])