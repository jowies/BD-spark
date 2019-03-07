from pyspark import sql, SparkConf, SparkContext
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

conf = SparkConf().setAppName("task_1")
sc = SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)

df_artists = sqlContext.read.csv("artists.csv")
df_artists_r = df_artists.select(*(col(x).alias(x + '_artists') for x in df_artists.columns))
df_albums = sqlContext.read.csv("albums.csv")
df_albums_r = df_albums.select(*(col(x).alias(x + '_albums') for x in df_albums.columns))

df_artists_r.show()

df_albums_r.show()

df = df_artists_r.join(df_albums_r, df_artists_r._c0_artists == df_albums_r._c1_albums)

df = df.withColumn("_c0_artists", df["_c0_artists"].cast(IntegerType()))
df.groupBy("_c0_artists").count().sort(["count", "_c0_artists"], ascending=[False, True]).coalesce(1).write.option("delimiter", "\t").csv("result_4.tsv")