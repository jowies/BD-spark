from pyspark import sql, SparkConf, SparkContext
from pyspark.sql.functions import udf, array, col
from pyspark.sql.types import DoubleType, IntegerType


conf = SparkConf().setAppName("task_1")
sc = SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)


avg = udf(lambda array: sum(array)/len(array), DoubleType())

df = sqlContext.read.csv("albums.csv")
df_artists = sqlContext.read.csv("artists.csv")
df_artists_r = df_artists.select(*(col(x).alias(x + '_artists') for x in df_artists.columns))

marksColumns = [col("_c7"), col("_c8"), col("_c9")]

averageFunc = sum(x for x in marksColumns)/len(marksColumns)

newdf = df.withColumn('total', averageFunc)

newdf = newdf.withColumn("_c0", df["_c0"].cast(IntegerType()))

topdf = newdf.sort(["total","_c0"], ascending=[False, True]).limit(10)

df_j = df_artists_r.join(topdf, df_artists_r._c0_artists == topdf._c1)

df_j.select("_c0", "total", "_c5_artists").show()

