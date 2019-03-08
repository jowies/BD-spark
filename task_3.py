from pyspark import sql, SparkConf, SparkContext

conf = SparkConf().setAppName("task_1")
sc = SparkContext(conf=conf)


raw_data = sc.textFile("artists.csv", 1)

split = raw_data.map(lambda x: (x.split(",")[5], 1))


res = split.groupByKey().mapValues(len).sortBy(lambda x: (x[1] * -1, x[0]), True)

res.map(lambda y: '{var1}\t{var2}'.format(var1=y[0].encode("utf-8"), var2=y[1])) \
  .coalesce(1).saveAsTextFile("result_3.tsv")