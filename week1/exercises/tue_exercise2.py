from pyspark import SparkContext

sc = SparkContext("local[*]", "RDDActions")

numbers = sc.parallelize([10, 5, 8, 3, 15, 12, 7, 20, 1, 9])
#Task
# collect() - Get all elements
all_nums = numbers.collect()
print(f"All numbers: {all_nums}")

#count() -count elements
count = numbers.count()
print(f"Count: {count}")

#first() - Get first element
first = numbers.first()
print(f"First: {first}")

#take(n) - get first n elements
first_three = numbers.take(3)
print(f"First 3: {first_three}")

#top(n) - get largest n elements
top_three = numbers.top(3)
print(f"Top 3: {top_three}")

#takeOrder(n) - get smallest n elements
smallest_three = numbers.takeOrdered(3)
print(f"Smallest 3: {smallest_three}")

#reduce() - sum all numbers
total = numbers.reduce(lambda x, y: x + y)
print(f"Sum: {total}")

#reduce() find maximum
maximum = numbers.reduce(lambda x, y: x if x > y else y)
print(f"Max: {maximum}")

#reduce() find minimum
minimum = numbers.reduce(lambda x, y: x if x < y else y)
print(f"Min: {minimum}")

#fold() - sum with zero value
folded_sum = numbers.fold(0, lambda x, y: x + y)
print(f"Folded sum: {folded_sum}")

# Given: colors with duplicates
colors = sc.parallelize(["red", "blue", "red", "green", "blue", "red", "yellow"])

#count occurences of each color
color_counts = (
    colors.map(lambda color: (color, 1))
    .reduceByKey(lambda x, y: x + y).collect()
)
print(f"Color counts: {dict(color_counts)}")