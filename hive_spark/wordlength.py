from pyspark.sql import SparkSession

########## overall word length & by sources of summarisation dataset ########## 
###### wordlength for SUMMARY ######
# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("SummaryLengthByPublisherType") \
    .enableHiveSupport() \
    .getOrCreate()

hive_query_content_length_average = """
SELECT 
    ROUND(AVG(LENGTH(summary)), 2) AS avg_summary_length,
    MIN(LENGTH(summary)) AS min_summary_length,
    MAX(LENGTH(summary)) AS max_summary_length
FROM news
"""

# Define the Hive SQL query to calculate summary length statistics by publisher_type
hive_query_content_length_by_publisher = """
SELECT 
    publisher_type AS summarisation_of_dataset,
    ROUND(AVG(LENGTH(summary)), 2) AS avg_summary_length,
    MIN(LENGTH(summary)) AS min_summary_length,
    MAX(LENGTH(summary)) AS max_summary_length
FROM news
GROUP BY publisher_type
"""

# Execute the query using spark.sql
result_content_length_average_df = spark.sql(hive_query_content_length_average)
result_content_length_by_publisher_df = spark.sql(hive_query_content_length_by_publisher)

# Show the results
result_content_length_average_df.show(truncate=False)
result_content_length_by_publisher_df.show(truncate=False)


#### wordlength for CONTENT ######
spark = SparkSession.builder \
    .appName("ContentLengthByPublisherType") \
    .enableHiveSupport() \
    .getOrCreate()

hive_query_content_length_average = """
SELECT 
    ROUND(AVG(LENGTH(content)), 2) AS avg_content_length,
    MIN(LENGTH(content)) AS min_content_length,
    MAX(LENGTH(content)) AS max_content_length
FROM news
"""

# Define the Hive SQL query to calculate summary length statistics by publisher_type
hive_query_content_length_by_publisher = """
SELECT 
    publisher_type AS summarisation_of_dataset,
    ROUND(AVG(LENGTH(content)), 2) AS avg_content_length,
    MIN(LENGTH(content)) AS min_content_length,
    MAX(LENGTH(content)) AS max_content_length
FROM news
GROUP BY publisher_type
"""

# Execute the query using spark.sql
result_content_length_average_df = spark.sql(hive_query_content_length_average)
result_content_length_by_publisher_df = spark.sql(hive_query_content_length_by_publisher)

# Show the results
result_content_length_average_df.show(truncate=False)
result_content_length_by_publisher_df.show(truncate=False)

# Stop the Spark session
spark.stop()

