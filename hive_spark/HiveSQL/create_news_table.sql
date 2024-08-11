-- create table news after loading in 01_initialise_data_to_hive.py
CREATE EXTERNAL TABLE IF NOT EXISTS news (
    ID STRING,
    Record_ID STRING,
    Content STRING,
    Summary STRING,
    Publisher_type STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET
LOCATION 'file:///home/hadoop/asm1/spark-warehouse/news';