from pyspark import sql, SparkConf, SparkContext
from operator import add

conf = SparkConf().setAppName("task_1")
sc = SparkContext(conf=conf)

albums = sc.textFile("albums.csv", 1)

artists = sc.textFile("artists.csv", 1)

def tup(x):
  s = x.split(",")
  critics = [float(s[7]), float(s[8]), float(s[9])]
  return (s[1], (s[0],sum(critics)/3))

albums_calculated = albums.map(tup)

res = albums_calculated.sortBy(lambda x: x[1][1], False).take(10)

top = sc.parallelize(res)

artists_c = artists.map(lambda x: (x.split(",")[0], x.split(",")[5]))

joined = artists_c.join(top)

joined.map(lambda y: '{var1}\t{var2}\t{var3}'.format(var1=y[1][1][0], var2=y[1][1][1], var3=y[1][0].encode("utf-8"))) \
  .coalesce(1).saveAsTextFile("result_7.tsv")
