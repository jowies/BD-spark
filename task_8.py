from pyspark import sql, SparkConf, SparkContext
from operator import add

conf = SparkConf().setAppName("task_1")
sc = SparkContext(conf=conf)

albums = sc.textFile("albums.csv", 1)

artists = sc.textFile("artists.csv", 1)

def tup(x):
  s = x.split(",")
  return (s[1],float(s[8]))

albums_mtv = albums.map(tup).filter(lambda x: x[1] == 5.0)

artists_c = artists.map(lambda x: (x.split(",")[0], x.split(",")[1]))

joined = artists_c.join(albums_mtv)
no_dup = joined.map(lambda x: x[1][0]).distinct().sortBy(lambda x: x)

no_dup.map(lambda y: '{var1}'.format(var1=y)) \
  .coalesce(1).saveAsTextFile("result_8.tsv")
