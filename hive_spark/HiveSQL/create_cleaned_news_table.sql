-- create table for cleaned_news after preprocessing in 02_clean.py
CREATE EXTERNAL TABLE cleaned_news (
    id STRING,
    record_id STRING,
    cleaned_content STRING,
    cleaned_summary STRING,
    publisher_type STRING
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/cleaned_news';
