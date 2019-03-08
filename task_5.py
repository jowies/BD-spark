from pyspark import sql, SparkConf, SparkContext
from operator import add

conf = SparkConf().setAppName("task_1")
sc = SparkContext(conf=conf)

albums = sc.textFile("albums.csv", 1)

def tup(x):
  return (x.split(",")[3], int(x.split(",")[6]))

albums_aid = albums.map(tup)

res = albums_aid.reduceByKey(add).sortBy(lambda x: (x[1] * -1, x[0]), True)

res.map(lambda y: '{var1}\t{var2}'.format(var1=y[0], var2=y[1])) \
  .coalesce(1).saveAsTextFile("result_5.tsv")