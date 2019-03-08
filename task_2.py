from pyspark import sql, SparkConf, SparkContext

conf = SparkConf().setAppName("task_1")
sc = SparkContext(conf=conf)

raw_data = sc.textFile("artists.csv", 1)

split = raw_data.map(lambda x: x.split(",")[4])

num = split.map(lambda x: int(x))

print(num.min())