from pyspark import sql, SparkConf, SparkContext
from pyspark.sql.functions import countDistinct, sum


conf = SparkConf().setAppName("task_1")
sc = SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)

df = sqlContext.read.csv("albums.csv")
df.show()

df.groupBy("_c3").agg(sum("_c6")).sort(["sum(_c6)", "_c3"], ascending=[False, True]).coalesce(1).write.option("delimiter", "\t").csv("result_5.tsv")