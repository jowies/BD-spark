from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("task_1")
sc = SparkContext(conf=conf)

raw_data = sc.textFile("albums.csv", 1)

split = raw_data.map(lambda x: x.split(",")[3])

print(split.distinct().count())