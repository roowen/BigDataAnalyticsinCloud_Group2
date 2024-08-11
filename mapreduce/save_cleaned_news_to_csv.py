# Read Hive table to csv 
from pyspark.sql import SparkSession

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("ExportCleanedNewsToCSV") \
    .enableHiveSupport() \
    .getOrCreate()

# Read the cleaned_news Hive table
cleaned_news_df = spark.sql("SELECT * FROM cleaned_news")

# Path to the output CSV file in the local file system
csv_output_path = "file:///home/hadoop/asm1/dataset"

# Write the DataFrame directly to a CSV file in the local file system
# Use UTF-8 encoding to handle special characters
cleaned_news_df.coalesce(1).write.option("header", "true").option("encoding", "UTF-8").mode("overwrite").csv(csv_output_path)

# Stop the Spark session
spark.stop()
