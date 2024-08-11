-- Analyze the distribution of news articles by publisher type
SELECT publisher_type, COUNT(*) AS article_count, 
       ROUND((COUNT(*) / (SELECT COUNT(*) FROM news) * 100), 2) AS percentage
FROM news
GROUP BY publisher_type;
