--------------------------------- WordCount before preprocessing -----------------------------------------------
-- Wordcount for Summary 
CREATE TABLE summary_words (
    word STRING
);

INSERT INTO summary_words
SELECT explode(split(summary, '\\s+')) AS word
FROM news;

SELECT word, COUNT(*) AS summary_count
FROM summary_words
GROUP BY word
ORDER BY summary_count DESC
LIMIT 20;

-- Wordcount for Content
Codes to perform wordcount for Contents:
CREATE TABLE content_words (
    word STRING
);

INSERT INTO content_words
SELECT explode(split(Content, '\\s+')) AS word
FROM news;

SELECT word, COUNT(*) AS content_count
FROM content_words
GROUP BY word
ORDER BY content_count DESC
LIMIT 10;

--------------------------------- WordCount before preprocessing -----------------------------------------------
-- Wordcount for Summary 
CREATE TABLE cleaned_content_words (
    word STRING
);

INSERT INTO cleaned_content_words
SELECT explode(split(cleaned_content, '\\s+')) AS word
FROM cleaned_news;

SELECT word, COUNT(*) AS content_count
FROM  cleaned_content_words
GROUP BY word
ORDER BY content_count DESC
LIMIT 10;

-- Wordcount for Summary 
CREATE TABLE cleaned_summmary_words (
    word STRING
);

INSERT INTO cleaned_summmary_words
SELECT explode(split(cleaned_summary, '\\s+')) AS word
FROM cleaned_news;

SELECT word, COUNT(*) AS content_count
FROM  cleaned_summmary_words
GROUP BY word
ORDER BY content_count DESC
LIMIT 10;
