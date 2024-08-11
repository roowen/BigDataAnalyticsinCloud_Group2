from pyspark.sql import SparkSession
from textblob import TextBlob
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("SentimentAnalysis") \
    .enableHiveSupport() \
    .getOrCreate()

# Load data from Hive table
df = spark.sql("SELECT * FROM cleaned_news")

# Show the first few rows of the DataFrame
df.show(5, truncate=False)

# Define the sentiment analysis function
def get_sentiment(text):
    try:
        if text is not None:
            blob = TextBlob(text)
            return float(blob.sentiment.polarity)
    except Exception as e:
        return None

# Register UDF for sentiment analysis
sentiment_udf = udf(get_sentiment, FloatType())

# Apply the sentiment analysis UDF, it add on a new column called sentiment
df_sentiment = df.withColumn("sentiment", sentiment_udf(df['cleaned_content']))

# Show the DataFrame with sentiment scores
df_sentiment.show(5, truncate=False)

# Save the DataFrame with sentiment analysis results to a new Hive table as parquet file in HDFS 
df_sentiment.write.mode("overwrite").saveAsTable("news_with_sentiment")

# Stop the Spark session
spark.stop()





