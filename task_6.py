from pyspark import sql, SparkConf, SparkContext
from pyspark.sql.functions import udf, array, col
from pyspark.sql.types import DoubleType, IntegerType


conf = SparkConf().setAppName("task_1")
sc = SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)


avg = udf(lambda array: sum(array)/len(array), DoubleType())

df = sqlContext.read.csv("albums.csv")

marksColumns = [col("_c7"), col("_c8"), col("_c9")]

averageFunc = sum(x for x in marksColumns)/len(marksColumns)

newdf = df.withColumn('total', averageFunc)

newdf = newdf.withColumn("_c0", df["_c0"].cast(IntegerType()))

topdf = newdf.sort(["total","_c0"], ascending=[False, True]).limit(10)

topdf.select("_c0", "total").show()
