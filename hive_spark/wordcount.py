from pyspark.sql import SparkSession
########## overall word count & by sources of summarisation dataset ########## 
# word count overall and by sources of summaisation dataset

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("WordCountAnalysis") \
    .enableHiveSupport() \
    .getOrCreate()

# Define the Hive SQL query
hive_query_wordcount_average = """
SELECT 
    ROUND(AVG(SIZE(SPLIT(content, '\\s+'))), 2) AS total_content_word_count,
    ROUND(AVG(SIZE(SPLIT(summary, '\\s+'))), 2) AS total_summary_word_count
FROM news
"""

hive_query_wordcount_by_publisher = """
SELECT 
    publisher_type AS summarisation_of_dataset,
    ROUND(AVG(SIZE(SPLIT(content, '\\s+'))), 2) AS total_content_word_count,
    ROUND(AVG(SIZE(SPLIT(summary, '\\s+'))), 2) AS total_summary_word_count
FROM news
GROUP BY publisher_type
"""

# Execute the query using spark.sql
hive_query_wordcount_average_df = spark.sql(hive_query_wordcount_average)
hive_query_wordcount_by_publisher_df = spark.sql(hive_query_wordcount_by_publisher)

# Show the results
hive_query_wordcount_average_df.show()
hive_query_wordcount_by_publisher_df.show()

# Stop the Spark session
spark.stop()

