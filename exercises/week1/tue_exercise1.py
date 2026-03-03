from pyspark import SparkContext

sc = SparkContext("local[*]", "RDDBasics")
#1. create RDD from a python list
numbers = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
print(f"Numbers: {numbers.collect()}")

# 2. Create RDD with explicit partition
numbers_4_partition = sc.parallelize([1,2,3,4,5,6,7,8,9,10], 4)
print(f"Numbers with 4 partitions: {numbers_4_partition.glom().collect()}")
print(f"Partiticons: {numbers_4_partition.getNumPartitions()}")

#3. Create RDD from a range
range_rdd = sc.parallelize(range(1, 101))
print(f" Range RDD (first 10 elements): {range_rdd.take(10)}")
print(f"Total elements in range RDD: {range_rdd.count()}")

#Task: 
# square each number
squared = numbers.map(lambda x: x * x)
print("Squared:", squared.collect())

#convert to strings with prefix
prefixed = numbers.map(lambda x: f"num_{x}")
print("Prefixed:", prefixed.collect())

#Task:
# keep only even numbers
evens = numbers.filter(lambda x: x % 2 ==0)
print("Evens: ", evens.collect())

#keep numbers greater than 5
greater_than_5 = numbers.filter(lambda x: x > 5)
print("Greater than 5:", greater_than_5.collect())

#Combine - even AND greater than 5
combined = numbers.filter(lambda x: x % 2 == 0 and x > 5)
print("Combined (even and >5):", combined.collect())

#Task
# Apply flatMap() Transformation
sentences = sc.parallelize([
    "Hello World",
    "Apache Spark is Fast",
    "PySpark is Python plus Spark"
])
#split into words (use flatMap)
words = sentences.flatMap(lambda sentence: sentence.split(" "))
print("Words:", words.collect())

#create pairs of (word, length)
words_lengths = words.map(lambda word: (word, len(word)))
print("Word Lengths:", words_lengths.collect())

#Task
logs = sc.parallelize([
    "INFO: User logged in",
    "ERROR: Connection failed",
    "INFO: Data processed",
    "ERROR: Timeout occurred",
    "DEBUG: Cache hit"
])
error_words = (
    logs.filter (lambda line: line.startswith("ERROR"))
    .flatMap(lambda line: line.split(" "))
    .map(lambda word: word.upper())
)
print("Error Words:", error_words.collect())
sc.stop()

