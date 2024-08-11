from pyspark.sql import SparkSession

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("SentimentAnalysisTasks") \
    .enableHiveSupport() \
    .getOrCreate()

# 1. Read the first 10 rows of the news_with_sentiment table and rename publisher_type as dataset_summaries
df = spark.sql("""
    SELECT 
        publisher_type AS summarisation_of_dataset, 
        cleaned_content, 
        cleaned_summary,
        sentiment
    FROM 
        news_with_sentiment 
    LIMIT 10
""")
df.show(truncate=True)

# 2. Calculate the average sentiment score of all rows
average_sentiment = spark.sql("SELECT ROUND(AVG(sentiment), 2) AS average_sentiment_score FROM news_with_sentiment")
average_sentiment.show()

# 3. Calculate the average sentiment score grouped by publisher_type
average_sentiment_by_publisher = spark.sql("""
    SELECT publisher_type AS summarisation_of_dataset, ROUND(AVG(sentiment), 2) AS average_sentiment_score
    FROM news_with_sentiment
    GROUP BY publisher_type
""")
average_sentiment_by_publisher.show()

# Stop the Spark session
spark.stop()







