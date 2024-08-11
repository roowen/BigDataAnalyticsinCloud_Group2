# -*- coding: utf-8 -*-
"""
Created on Tue Aug  6 23:43:42 2024

@author: sooky
"""

import pandas as pd
import re
from collections import Counter
from nltk.corpus import stopwords
import nltk
import datetime

# Ensure that the stopwords dataset is downloaded
nltk.download('stopwords')

# Set of English stopwords
stop_words = set(stopwords.words('english'))

start_time= datetime.datetime.now()

# Load the dataset
df = pd.read_csv(r"C:\Users\sooky\Desktop\school\year3\y3s3\big data\data.csv")

end_read = datetime.datetime.now()

print("Reading data csv complete.")
print(f"Time for reading: {(end_read - start_time).total_seconds():.2f} seconds")

# Keep only the required columns
df = df[['Content', 'Summary', 'Dataset']]

# Function to clean text
def clean_text(text):
    # Remove new lines and lowercase text
    text = str(text)
    text = text.replace('\n', '').lower()
    # Remove punctuation
    text = re.sub(r'[^\w\s]', '', text)
    # Remove stopwords
    text = ' '.join([word for word in text.split() if word not in stop_words])
    return text

# Apply the cleaning function
df['Content'] = df['Content'].apply(clean_text)
df['Summary'] = df['Summary'].apply(clean_text)

end_clean= datetime.datetime.now()
print(f'Starting cleaning at {start_time}')
print(f'Time end for cleaning: {end_clean}')
print(f"Time for cleaning: {(end_clean - end_read).total_seconds():.2f} seconds")


start_count= datetime.datetime.now()

# Function to tokenize text
def tokenize(text):
    # Tokenize text
    tokens = text.split()
    return tokens

# Apply the cleaning and tokenization function to 'Content' and 'Summary'
df['Content_tokens'] = df['Content'].apply(tokenize)
df['Summary_tokens'] = df['Summary'].apply(tokenize)


# Apply the tokenization function
all_words_sum = [word for sublist in df['Summary_tokens'] for word in sublist]
# Count word frequencies
word_counts_sum = Counter(all_words_sum)
# Get the most common words
most_common_words_sum = word_counts_sum.most_common(20)  # Adjust the number as needed


##content
all_words_con = [word for sublist in df['Content_tokens'] for word in sublist]
# Count word frequencies
word_counts_con = Counter(all_words_con)
# Get the most common words
most_common_words_con = word_counts_con.most_common(20)  # Adjust the number as needed

end_count = datetime.datetime.now()

print(f'Start count at {start_count}')
print(f'End count at {end_count}')
print(f"Time for counting: {(end_count - start_count).total_seconds():.2f} seconds")


# Print the most common words and their counts
print("Most common words in content and their frequencies:")
for word, frequency in most_common_words_con:
    print(f"{word}: {frequency}")

# Print the most common words and their counts
print("Most common words in Summary and their frequencies:")
for word, frequency in most_common_words_sum:
    print(f"{word}: {frequency}")
    
    






