from pyspark import sql, SparkConf, SparkContext

conf = SparkConf().setAppName("task_1")
sc = SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)

df_albums = sqlContext.read.csv("albums.csv")\
  .withColumnRenamed("_c0", "id")\
  .withColumnRenamed("_c1", "artist_id")\
  .withColumnRenamed("_c2", "album_title")\
  .withColumnRenamed("_c3", "genre")\
  .withColumnRenamed("_c4", "year_of_pub")\
  .withColumnRenamed("_c5", "num_of_tracks")\
  .withColumnRenamed("_c6", "num_of_sales")\
  .withColumnRenamed("_c7", "rolling_stone_critic")\
  .withColumnRenamed("_c8", "mtv_critic")\
  .withColumnRenamed("_c9", "music_maniac_critic")

df_artists = sqlContext.read.csv("artists.csv")\
  .withColumnRenamed("_c0", "id")\
  .withColumnRenamed("_c1", "real_name")\
  .withColumnRenamed("_c2", "art_name")\
  .withColumnRenamed("_c3", "role")\
  .withColumnRenamed("_c4", "year_of_birth")\
  .withColumnRenamed("_c5", "country")\
  .withColumnRenamed("_c6", "city")\
  .withColumnRenamed("_c7", "email")\
  .withColumnRenamed("_c8", "zip_code")

a = df_artists.select("id").distinct().count()

b = df_albums.select("id").distinct().count()

c = df_albums.select("genre").distinct().count()

d = df_artists.select("country").distinct().count()

e = df_albums.withColumn("year_of_pub", df_albums["year_of_pub"].cast(sql.types.IntegerType())).agg({"year_of_pub": "min"}).collect()[0][0]

f = df_albums.withColumn("year_of_pub", df_albums["year_of_pub"].cast(sql.types.IntegerType())).agg({"year_of_pub": "max"}).collect()[0][0]

g = df_artists.withColumn("year_of_birth", df_artists["year_of_birth"].cast(sql.types.IntegerType())).agg({"year_of_birth": "min"}).collect()[0][0]

h = df_artists.withColumn("year_of_birth", df_artists["year_of_birth"].cast(sql.types.IntegerType())).agg({"year_of_birth": "max"}).collect()[0][0]

df = [("a", a), ("b", b), ("c", c), ("d", d), ("e", e), ("f", f), ("g", g), ("h", h)]

sqlContext.createDataFrame(df, ["task", "result"]).show()
