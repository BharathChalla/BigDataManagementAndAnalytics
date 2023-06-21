import time

from pyspark.sql.functions import count, desc, avg, sum


ratings = spark.read.options(header='False', inferSchema='True', delimiter='\t').csv('/user/hadoop/ml-100k/u.data')	
items = spark.read.options(header='False', inferSchema='True', delimiter='|').csv('/user/hadoop/ml-100k/u.item')	
movieusers = spark.read.options(header='False', inferSchema='True', delimiter='|').csv('/user/hadoop/ml-100k/u.user')	

ratings = ratings.toDF("userid", "itemid", "rating", "timestamp")
ratings.printSchema()

items = items.toDF("movieid", "movie", "releaseDate", "videoReleaseDate", "IMDbURL", "unknown", "Action", "Adventure", "Animation", "Childrens", "Comedy", "Crime", "Documentary", "Drama", "Fantasy", "FilmNoir", "Horror", "Musical", "Mystery", "Romance", "SciFi", "Thriller", "War", "Western")
items.printSchema()

movieUsers = movieUsers.toDF("userid", "age", "gender", "occupation", "zipcode")
movieUsers.printSchema()

# Q1
start_time = time.time()
ratings.groupBy("userid").agg(count("rating").alias("ratingCount")).join(movieUsers, "userid").select("age", "gender", "occupation").orderBy(desc("ratingCount")).show(1)
print(f"Execution time: {time.time() - start_time} seconds")
print()

# Q2
start_time = time.time()
movieUsers.join(ratings, "userid").groupBy("occupation").agg(count("userid").alias("userCount")).orderBy(desc("userCount")).show(21)
print(f"Execution time: {time.time() - start_time} seconds")
print()

# Q3
start_time = time.time()
items.agg(sum("Action").alias("Action"), sum("Adventure").alias("Adventure"), sum("Animation").alias("Animation"), sum("Childrens").alias("Childrens"), sum("Comedy").alias("Comedy"), sum("Crime").alias("Crime"), sum("Documentary").alias("Documentary"), sum("Drama").alias("Drama"), sum("Fantasy").alias("Fantasy"), sum("FilmNoir").alias("FilmNoir"), sum("Horror").alias("Horror"), sum("Musical").alias("Musical"), sum("Mystery").alias("Mystery"), sum("Romance").alias("Romance"), sum("SciFi").alias("SciFi"), sum("Thriller").alias("Thriller"), sum("War").alias("War"), sum("Western").alias("Western"), sum("unknown").alias("unknown"),).show()
print(f"Execution time: {time.time() - start_time} seconds")
print()

# Q4
start_time = time.time()
movieUsers.filter("occupation like 'scientist'").agg(count("userid").alias("NumberOfScientists"), avg("age").alias("AverageAge"), ).show()
print(f"Execution time: {time.time() - start_time} seconds")
print()