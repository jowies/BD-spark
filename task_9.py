from pyspark import sql, SparkConf, SparkContext
from operator import add

conf = SparkConf().setAppName("task_1")
sc = SparkContext(conf=conf)

albums = sc.textFile("albums.csv", 1)

artists = sc.textFile("artists.csv", 1)

def tup(x):
  s = x.split(",")
  return (s[1],float(s[8]))

albums_mtv =  albums.map(tup) \
    .mapValues(lambda v: (v, 1)) \
    .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])) \
    .mapValues(lambda v: v[0]/v[1]) \

artists_c = artists.map(lambda x: (x.split(",")[0], [x.split(",")[1], x.split(",")[5]])).filter(lambda x: x[1][1] == "Norway")

joined = artists_c.join(albums_mtv).sortBy(lambda x: (x[1][1]*-1, x[1][0][0]), True)

joined.map(lambda y: '{var1}\t{var2}\t{var3}'.format(var1=y[1][0][0], var2=y[1][0][1], var3=y[1][1])) \
  .coalesce(1).saveAsTextFile("result_9.tsv")