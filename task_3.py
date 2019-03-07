from pyspark import sql, SparkConf, SparkContext

conf = SparkConf().setAppName("task_1")
sc = SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)

df = sqlContext.read.csv("artists.csv")


df.groupBy('_c5').count().sort(["count", "_c5"], ascending=[False, True]).coalesce(1).write.option("delimiter", "\t").csv("result_3.tsv")