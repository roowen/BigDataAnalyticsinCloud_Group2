from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import string
import nltk
from nltk.corpus import stopwords

# Download stopwords from NLTK
nltk.download('stopwords')
stop_words = set(stopwords.words('english'))

# Define UDFs
# perform lowercase
def to_lowercase(text):
    if text is not None:
        return text.lower()
    return text

# remove punctuation
def remove_punctuation(text):
    if text is not None:
        for punct in string.punctuation:
            text = text.replace(punct, '')
    return text

# remove stopwords
def remove_stopwords(text):
    if text is not None:
        return ' '.join([word for word in text.split() if word.lower() not in stop_words])
    return text

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("TextPreprocessing") \
    .enableHiveSupport() \
    .getOrCreate()

# Register UDFs
to_lowercase_udf = udf(to_lowercase, StringType())
remove_punctuation_udf = udf(remove_punctuation, StringType())
remove_stopwords_udf = udf(remove_stopwords, StringType())

# Read the Hive table
df = spark.sql("SELECT * FROM news")

# Apply the UDFs in the correct order
df_cleaned = df.withColumn("cleaned_content", remove_stopwords_udf(remove_punctuation_udf(to_lowercase_udf(col("Content"))))) \
               .withColumn("cleaned_summary", remove_stopwords_udf(remove_punctuation_udf(to_lowercase_udf(col("Summary"))))) \
               .select("id", "record_id", "cleaned_content", "cleaned_summary", "publisher_type")

# Show the cleaned DataFrame
df_cleaned.show(truncate=False)

# Save the cleaned DataFrame back to Hive as a new table as parquet file
df_cleaned.write.mode("overwrite").saveAsTable("cleaned_news")

# Stop the Spark session
spark.stop()



