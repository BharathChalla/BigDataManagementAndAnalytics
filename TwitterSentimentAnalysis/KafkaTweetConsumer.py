import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from textblob import TextBlob
import re
from kafka import KafkaProducer


def cleanTweet(tweet: str) -> str:
    tweet = re.sub(r'http\S+', '', str(tweet))
    tweet = re.sub(r'bit.ly/\S+', '', str(tweet))
    tweet = tweet.strip('[link]')

    # remove users
    tweet = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))
    tweet = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    # remove puntuation
    my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@â'
    tweet = re.sub('[' + my_punctuation + ']+', ' ', str(tweet))

    # remove number
    tweet = re.sub('([0-9]+)', '', str(tweet))

    # remove hashtag
    tweet = re.sub('(#[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    return tweet


# text classification
def polarity_detection(text):
    return TextBlob(text).sentiment.polarity


def subjectivity_detection(text):
    return TextBlob(text).sentiment.subjectivity


def getSubjectivity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.subjectivity


# Create a function to get the polarity
def getPolarity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.polarity


def getSentiment(polarityValue: int) -> str:
    if polarityValue < 0:
        return 'Negative'
    elif polarityValue == 0:
        return 'Neutral'
    else:
        return 'Positive'

if __name__ == "__main__":
    # create Spark session
    spark = SparkSession \
        .builder \
        .appName("TwitterSentimentAnalysis").getOrCreate()

    # set log level

    spark.sparkContext.setLogLevel("ERROR")
    # read the tweet data from socket
    lines = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", sys.argv[1]) \
        .option("subscribe", sys.argv[2]) \
        .load() \
        #.selectExpr("CAST(value as STRING)", "timestamp")

    lines.printSchema()

    # Preprocess the data

    mySchema = StructType([StructField("text", StringType(), True)])
    values = lines.select(from_json(lines.value.cast("string"), mySchema).alias("tweet"))

    df1 = values.select("tweet.*")

    clean_tweets = F.udf(cleanTweet, StringType())

    raw_tweets = df1.withColumn('processed_text', clean_tweets(col("text")))

    subjectivity = F.udf(getSubjectivity, FloatType())
    polarity = F.udf(getPolarity, FloatType())
    sentiment = F.udf(getSentiment, StringType())

    subjectivity_tweets = raw_tweets.withColumn('subjectivity', subjectivity(col("processed_text")))
    polarity_tweets = subjectivity_tweets.withColumn("polarity", polarity(col("processed_text")))
    sentiment_tweets = polarity_tweets.withColumn("sentiment", sentiment(col("polarity")))

    print(type(sentiment_tweets))

    subjectivity_tweets.printSchema()
    polarity_tweets.printSchema()
    sentiment_tweets.printSchema()

    # subjectivity_tweets.take(10)
    # polarity_tweets.take(10)
    # sentiment_tweets.take(10)

    sentiment_tweets\
        .selectExpr("CAST(text AS STRING) AS key",  "CAST(sentiment AS STRING) AS value")\
        .writeStream\
        .format("kafka") \
        .outputMode("append")\
        .option("kafka.bootstrap.servers", sys.argv[1]) \
        .option("topic", sys.argv[3]) \
        .option("checkPointLocation", "file:///D://")\
        .start()\
        .awaitTermination()



