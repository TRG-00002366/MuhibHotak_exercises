from pyspark import SparkContext
import os

sc = SparkContext("local[*]", "SherlockHomeAnalysis")
lines_rdd = sc.textFile("mini_project/sherlock_holmes.txt")
import re
#task 1: text normalization

def normalize_text(line):
    #converting to lowercase
    line = line.lower()
    #remove punctuation(keep letters, numbers, and spaces)
    line = re.sub(r'[^a-z0-9\s]', '', line)
    return line
normalized_rdd = lines_rdd.map(normalize_text)
for line in normalized_rdd.take(5):
    print (line)

#task 2
words_rdd = normalized_rdd.flatMap(lambda line: line.split())
print(words_rdd.take(10))

#T3: filtering stop words
stopwords = {
    "the", "a", "an", "is", "of", "and", "to", "in", "that", 
    "this","it","for","on","with","in","as","was","at","by"
}
words_rdd = normalized_rdd.flatMap(lambda line: line.split())
filtered_words_rdd = words_rdd.filter(lambda word: word not in stopwords)
print(filtered_words_rdd.take(10))

#T4: T4: Character Counting
# Requirement: Calculate the total number of characters in the entire book.
# Example: Like counting every single letter and space in the physical book.
#counting characters including spaces
total_chars = normalized_rdd.map(lambda line: len(line)).reduce(lambda a, b: a+b)
print("Total characters in book:", total_chars)

# T5: Line Length Analysis
# Requirement: Find the longest line in the text and its character count.
# Example: Identifying the sentence where Sherlock gives his longest uninterrupted deduction.
#find the longest line
longest_line = normalized_rdd.max(key=lambda line: len(line))
#get its character count
longest_length = len(longest_line)
print("longest line character count:", longest_length)
print("longest line:", longest_line)

# T6: Word Search (Filtering)
# Requirement: Extract all lines that contain the word "Watson".
# Example: Like using 'Ctrl+F' to highlight every time Holmes addresses his assistant.
watson_lines = lines_rdd.filter(lambda line: "Watson" in line)
for line in watson_lines.take(10):
    print(line)

# T7: Unique Vocabulary Count
# Requirement: Count how many unique words are used throughout the book.
# Example: Determining if Doyle used 5,000 or 10,000 different words to write the story.
unique_word_count = words_rdd.distinct().count()
print("Total unique words in the book:", unique_word_count)

# T8: Top 10 Frequent Words
# Requirement: Identify the 10 most frequently used words in the book.
# Example: Finding out if "Sherlock" appears more often than "Holmes".
top_10 = filtered_words_rdd \
    .map(lambda w:(w,1)).reduceByKey(lambda a,b: a+b)\
    .sortBy(lambda x: x[1], ascending=False).take(10)
print(top_10)

# T9: Sentence Start Distribution
# Requirement: Find the first word of every line and count their occurrences.
# Example: Seeing how many times a description starts with the word "It" or "He".
#find first word from each line
first_words = normalized_rdd.filter(lambda line: line.strip() !="")\
.map(lambda line: line.split()[0])

#count occurrences
first_word_counts = first_words \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a,b: a+b)

#sort by frequency (highest first)
top_first_words = first_word_counts \
    .sortBy(lambda x: x[1], ascending=False).take(10)

print("Most common starting words:")
for word, count in top_first_words:
    print(word, count)

# T10: Average Word Length
# Requirement: Compute the average length of all words in the text.
# Example: Determining if the vocabulary is mostly made of short words (like "run") or long words (like "circumstantial").
#Total number of words
total_words = words_rdd.count()
#total number of characters in all words
total_characters = words_rdd.map(lambda word: len(word)).sum()
#Average word length
average_word_length = total_characters / total_words
print("Average word length:", average_word_length)

# T11: Distribution of Word Lengths
# Requirement: Count how many words have 1 letter, 2 letters, 3 letters, etc.
# Example: A tally chart showing "3-letter words: 500, 4-letter words: 800".

# Map each word to (word_length, 1)
word_length_counts = words_rdd.map(lambda word: (len(word), 1))\
    .reduceByKey(lambda a, b: a + b).sortByKey()
# collect and print results
print("Word Length Distribution:")
for length, count in word_length_counts.collect():
    print(f"{length}-letter words: {count}")

# T12: Specific Chapter Extraction
# Requirement: Filter and collect text only between "A SCANDAL IN BOHEMIA" and "THE RED-HEADED LEAGUE".
# Example: Ripping out just the pages related to the first story in the collection.

# combine entire book into one string
full_text = "\n".join(lines_rdd.collect())

# find start and end positions
start_marker = "A SCANDAL IN BOHEMIA"
end_marker = "THE RED-HEADED LEAGUE"

start_pos = full_text.find(start_marker)
end_pos = full_text.find(end_marker)

# extract chapter text
chapter_text = full_text[start_pos:end_pos]

# convert back to RDD if needed
chapter_rdd = sc.parallelize(chapter_text.split("\n"))

# preview first 20 lines
for line in chapter_rdd.take(20):
    print(line)

""""                                       """
#word length pairing
word_length_pairs = words_rdd.map(lambda word: (len(word), word))
print(word_length_pairs.take(10))

#Character Frequency (Pair RDD / ReduceByKey)
character_frequency = (
    normalized_rdd
    .flatMap(lambda line: list(line))
    .filter(lambda char: char.isalpha())
    .map(lambda char: (char, 1))
    .reduceByKey(lambda a, b: a + b)
    .sortByKey()
)
print(character_frequency.collect())

#grouped word lists (GroupByKey)
grouped_words = (
    words_rdd
    .filter(lambda word: word != "")
    .map(lambda word: (word[0], word))
    .groupByKey()
)

print({k: list(v)[:5] for k, v in grouped_words.take(5)})


#6 loading fom multiple resources and handle errors

import sys

file_path = "mini_project/sherlock_holmes.txt"

# Check if file exists before loading
if os.path.exists(file_path):
    lines_rdd = sc.textFile(file_path)
    print("File loaded successfully.")
else:
    print("ERROR: File not found at path:", file_path)
    print("Please verify the file location.")
    sys.exit(1)

# #7 saving analytics results

word_counts = (
    filtered_words_rdd
    .map(lambda word: (word, 1))
    .reduceByKey(lambda a, b: a + b)
)

output_path = "Holmes_WordCount_Results"

# Remove old folder if exists
import shutil
if os.path.exists(output_path):
    shutil.rmtree(output_path)

word_counts.saveAsTextFile(output_path)
