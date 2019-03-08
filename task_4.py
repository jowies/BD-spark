from pyspark import sql, SparkConf, SparkContext


conf = SparkConf().setAppName("task_1")
sc = SparkContext(conf=conf)

albums = sc.textFile("albums.csv", 1)

albums_aid = albums.map(lambda x: (x.split(",")[1], 1))

res = albums_aid.groupByKey().mapValues(len).sortBy(lambda x: (x[1] * -1, int(x[0])), True)

res.map(lambda y: '{var1}\t{var2}'.format(var1=y[0], var2=y[1])) \
  .coalesce(1).saveAsTextFile("result_4.tsv")