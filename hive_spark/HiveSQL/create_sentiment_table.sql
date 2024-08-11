--  create sentiment table from parquet file generated from generate_sentiment_score.py 
CREATE EXTERNAL TABLE news_with_sentiment (
  id STRING,
  record_id STRING,
  cleaned_content STRING,
  cleaned_summary STRING,
  publisher_type STRING,
  sentiment FLOAT
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/news_with_sentiment';
