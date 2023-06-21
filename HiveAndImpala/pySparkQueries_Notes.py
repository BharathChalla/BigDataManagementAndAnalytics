wget http://files.grouplens.org/datasets/movielens/ml-100k.zip
unzip ml-100k

# Ref: # Ref: https://stackoverflow.com/questions/40187956/hive-failed-semanticexception-line-123-invalid-path
chmod 754 ml-100k
chmod 777 ml-100k/*

# Need to wait till all the Master and Worker Nodes are started
hdfs dfs -put ml-100k /user/hadoop

ratings = spark.read.options(delimiter='\t').csv('/user/hadoop/ml-100k/u.data')
items = spark.read.options(delimiter='|').csv('/user/hadoop/ml-100k/u.item')
movieusers = spark.read.options(delimiter='|').csv('/user/hadoop/ml-100k/u.user')

from pyspark.sql.types import StructType, IntegerType, DoubleType, StringType

ratingSchema = StructType() \
      .add("UserID",IntegerType(),True) \
      .add("ItemID",IntegerType(),True) \
      .add("Rating",DoubleType(),True) \
      .add("TimeStamp",StringType(),True)
	  
["UserId", "ItemId", "Rating", "TimeStamp"]
	

df3 = spark.read.options(header='True', inferSchema='True', delimiter=',') \
  .csv("/tmp/resources/zipcodes.csv")
	
ratings = spark.read.options(header='False', inferSchema='True', delimiter='\t').csv('/user/hadoop/ml-100k/u.data')	
items = spark.read.options(header='False', inferSchema='True', delimiter='|').csv('/user/hadoop/ml-100k/u.item')	
movieusers = spark.read.options(header='False', inferSchema='True', delimiter='|').csv('/user/hadoop/ml-100k/u.user')	
	  

usersSchema = StructType() \
      .add("UserID",IntegerType(),True) \
      .add("Age",IntegerType(),True) \
      .add("Gender",StringType(),True) \
      .add("Occupation",StringType(),True) \
      .add("ZipCode",StringType(),True)
      

https://sparkbyexamples.com/pyspark/pyspark-read-csv-file-into-dataframe/


sum("unknown").alias("unknown"), sum("Action").alias("Action"), sum("Adventure").alias("Adventure"), sum("Animation").alias("Animation"), sum("Childrens").alias("Childrens"), sum("Comedy").alias("Comedy"), sum("Crime").alias("Crime"), sum("Documentary").alias("Documentary"), sum("Drama").alias("Drama"), sum("Fantasy").alias("Fantasy"), sum("FilmNoir").alias("FilmNoir"), sum("Horror").alias("Horror"), sum("Musical").alias("Musical"), sum("Mystery").alias("Mystery"), sum("Romance").alias("Romance"), sum("SciFi").alias("SciFi"), sum("Thriller").alias("Thriller"), sum("War").alias("War"), sum("Western").alias("Western")

# Ref: https://sparkbyexamples.com/pyspark/pyspark-read-csv-file-into-dataframe/

# Spark Conf - Ref: https://aws.amazon.com/premiumsupport/knowledge-center/emr-spark-classnotfoundexception/
# https://stackoverflow.com/questions/52502435/pyspark-error-compression-codec-com-hadoop-compression-lzo-lzocodec-not-found/52513488
# /usr/bin/spark-submit --conf="spark.driver.extraClassPath=/usr/lib/hadoop-lzo/lib/*" --conf="spark.executor.extraClassPath=/usr/customJars/*"  

# execution time of a query on PySpark - https://stackoverflow.com/a/71464125
import time
start_time = time.time()
movieUsers.filter("occupation like 'scientist'").agg(count("userid").alias("NumberOfScientists"), avg("age").alias("AverageAge"), ).show()
print(f"Execution time: {time.time() - start_time} seconds")
print()

# Q1
ratings.groupBy("userid").agg(count("rating").alias("ratingCount")).join(movieUsers, "userid").select("age", "gender", "occupation").orderBy(desc("ratingCount")).show(1)

# Q2
movieUsers.join(ratings, "userid").groupBy("occupation").agg(count("userid").alias("userCount")).orderBy(desc("userCount")).show(21)

# Q3
items.agg(sum("Action").alias("Action"), sum("Adventure").alias("Adventure"), sum("Animation").alias("Animation"), sum("Childrens").alias("Childrens"), sum("Comedy").alias("Comedy"), sum("Crime").alias("Crime"), sum("Documentary").alias("Documentary"), sum("Drama").alias("Drama"), sum("Fantasy").alias("Fantasy"), sum("FilmNoir").alias("FilmNoir"), sum("Horror").alias("Horror"), sum("Musical").alias("Musical"), sum("Mystery").alias("Mystery"), sum("Romance").alias("Romance"), sum("SciFi").alias("SciFi"), sum("Thriller").alias("Thriller"), sum("War").alias("War"), sum("Western").alias("Western"), sum("unknown").alias("unknown"),).show()

# Q4
movieUsers.filter("occupation like 'scientist'").agg(count("userid").alias("NumberOfScientists"), avg("age").alias("AverageAge"), ).show()