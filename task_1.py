from pyspark import sql, SparkConf, SparkContext

conf = SparkConf().setAppName("task_1")
sc = SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)

df = sqlContext.read.csv("albums.csv")
print(df.select("_c3").distinct().count())