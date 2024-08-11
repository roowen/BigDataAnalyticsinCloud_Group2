### Derive Compression Ratiio ###
from pyspark.sql import SparkSession
from pyspark.sql.functions import length, split, size, col, round, sum

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("SummaryCompressionRatio") \
    .enableHiveSupport() \
    .getOrCreate()

# Load the data from the Hive table (assuming table name is `news`)
df = spark.sql("SELECT publisher_type, content, summary FROM news")

# Calculate word counts
df_word_counts = df.withColumn("content_word_count", size(split(col("content"), "\\s+"))) \
                   .withColumn("summary_word_count", size(split(col("summary"), "\\s+")))

# Calculate the overall compression ratio
df_compression_ratio_overall = df_word_counts.agg(
    round((sum(col("summary_word_count")) / sum(col("content_word_count"))), 4).alias("overall_compression_ratio")
)

# Show the overall compression ratio
df_compression_ratio_overall.show(truncate=False)

# Rename `publisher_type` to `summarisation_of_dataset`
df_word_counts = df_word_counts.withColumnRenamed("publisher_type", "summarisation_of_dataset")

# Calculate the compression ratio by summarisation_of_dataset
df_compression_ratio_by_publisher = df_word_counts.groupBy("summarisation_of_dataset").agg(
    round((sum(col("summary_word_count")) / sum(col("content_word_count"))), 4).alias("compression_ratio_by_publisher")
)

# Show the compression ratio by summarisation_of_dataset
df_compression_ratio_by_publisher.show(truncate=False)

# Stop the Spark session
spark.stop()


