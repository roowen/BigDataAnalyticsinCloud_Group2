from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.types import StructType, StructField, StringType

input_file = "file:///home/hadoop/asm1/dataset/data.csv"


###### read data and replace new line as table 'news' sss
def initialise_data_to_hive():
    spark = SparkSession.builder.appName("ReadCSV_newlineremoval").getOrCreate()

    # Define the schema 
    schema = StructType([
        StructField("ID", StringType(), True),
        StructField("Record_ID", StringType(), True),
        StructField("Content", StringType(), True),
        StructField("Summary", StringType(), True),
        StructField("Publisher_type", StringType(), True)
    ])

    # read the csv with multiline set to True and escape line break
    df_news = spark.read.option("quote", "\"")\
        .option("multiline", "True")\
        .option("escape", "\"")\
        .csv(input_file, header=True, encoding='ISO-8859-1')

    # change the name of header
    df_news = df_news.toDF('ID', 'Record_ID', 'Content', 'Summary', 'Publisher_type')

    #Print the total number of rows
    print("Total number of rows read:", df_news.count())

    # Replacing new line as blank space for content and summary column 

    dfNews_cleaned = df_news.withColumn("Content", regexp_replace(col("Content"), '\n', ' ')) \
                .withColumn("Summary", regexp_replace(col("Summary"), '\n', ' '))

    # Save to hive table 
    dfNews_cleaned.write.mode('overwrite').saveAsTable("news")

    #read and print the table
    print("#################### Successfully store into Hive ####################################")
    df_hive = spark.read.table('news')
    df_hive.show()

    # Stop the Spark session
    spark.stop()


# run the main function
if __name__ == '__main__':
    initialise_data_to_hive()