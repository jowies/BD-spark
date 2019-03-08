from pyspark import sql, SparkConf, SparkContext
from operator import add

conf = SparkConf().setAppName("task_1")
sc = SparkContext(conf=conf)

albums = sc.textFile("albums.csv", 1)

def tup(x):
  s = x.split(",")
  critics = [float(s[7]), float(s[8]), float(s[9])]
  return (s[0], sum(critics)/3)

albums_calculated = albums.map(tup)

res = albums_calculated.sortBy(lambda x: x[1], False).take(10)

sc.parallelize(res).map(lambda y: '{var1}\t{var2}'.format(var1=y[0], var2=y[1])) \
  .coalesce(1).saveAsTextFile("result_6.tsv")
