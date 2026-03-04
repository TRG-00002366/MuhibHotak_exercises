"""
Exercise: Set Operations
========================
Week 2, Wednesday

Practice union, intersect, except operations on customer data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: Set Ops").master("local[*]").getOrCreate()

# January customers
jan_customers = spark.createDataFrame([
    (1, "Alice", "alice@email.com"),
    (2, "Bob", "bob@email.com"),
    (3, "Charlie", "charlie@email.com"),
    (4, "Diana", "diana@email.com")
], ["customer_id", "name", "email"])

# February customers  
feb_customers = spark.createDataFrame([
    (2, "Bob", "bob@email.com"),
    (4, "Diana", "diana@email.com"),
    (5, "Eve", "eve@email.com"),
    (6, "Frank", "frank@email.com")
], ["customer_id", "name", "email"])

# March customers (different column order!)
mar_customers = spark.createDataFrame([
    ("grace@email.com", "Grace", 7),
    ("henry@email.com", "Henry", 8),
    ("bob@email.com", "Bob", 2)  # Returning customer
], ["email", "name", "customer_id"])

print("=== Exercise: Set Operations ===")
print("\nJanuary Customers:")
jan_customers.show()
print("February Customers:")
feb_customers.show()
print("March Customers (different column order!):")
mar_customers.show()

# =============================================================================
# TASK 1: Union Operations (20 mins)
# =============================================================================

print("\n--- Task 1: Union ---")

# TODO 1a: Union January and February customers (keep duplicates)
jan_feb_union =jan_customers.union(feb_customers)
jan_feb_union.show()

# TODO 1b: Union January and February, then remove duplicates
jan_feb_unique=jan_feb_union.distinct()
jan_feb_unique.show()

# TODO 1c: Try union with March customers - what happens?
# Use unionByName to fix it
all_three=jan_customers.unionByName(feb_customers).unionByName(mar_customers)
all_three.show()

# TODO 1d: How many unique customers do you have across all three months?
unique_customers= all_three.dropDuplicates(["customer_id"])
print("Total unique customers:", unique_customers.count())
# =============================================================================
# TASK 2: Intersect (15 mins)
# =============================================================================

print("\n--- Task 2: Intersect ---")

# TODO 2a: Find customers who appear in BOTH January AND February
returning_customers = jan_customers.intersect(feb_customers)
returning_customers.show()

# TODO 2b: Verify the result makes sense - who are the returning customers?
# +-----------+-----+---------------+
# |customer_id| name|          email|
# +-----------+-----+---------------+
# |          4|Diana|diana@email.com|
# |          2|  Bob|  bob@email.com|
# +-----------+-----+---------------+

# =============================================================================
# TASK 3: Subtract/Except (15 mins)
# =============================================================================

print("\n--- Task 3: Subtract/Except ---")

# TODO 3a: Find customers in January who did NOT return in February
jan_not_returned = jan_customers.subtract(feb_customers)
jan_not_returned.show()

# TODO 3b: Find NEW customers in February (not in January)
feb_new = feb_customers.subtract(jan_customers)
feb_new.show()

# TODO 3c: Business question: What is the customer churn from Jan to Feb?
# Answer in a comment:customer churn = customers in january who did not return in feb
# 


# =============================================================================
# TASK 4: Distinct and DropDuplicates (15 mins)
# =============================================================================

print("\n--- Task 4: Deduplication ---")

# Combined data with duplicates
all_data = jan_customers.union(feb_customers)

# TODO 4a: Use distinct() to remove exact duplicate rows
distinct_row= all_data.distinct()
distinct_row.show()

# TODO 4b: Use dropDuplicates() on email column only
# (Keep first occurrence of each email)
dedup_by_email = all_data.dropDuplicates(["email"])
dedup_by_email.show()

# TODO 4c: What is the difference between distinct() and dropDuplicates()?
# Answer in a comment: distinct removes completely identical ros.
#dropDuplicates(["col"]) removes rows that share the same value


# =============================================================================
# CHALLENGE: Data Reconciliation (20 mins)
# =============================================================================

print("\n--- Challenge: Data Reconciliation ---")

# System A data (source)
source = spark.createDataFrame([
    (1, "Product A", 100),
    (2, "Product B", 200),
    (3, "Product C", 300)
], ["id", "name", "price"])

# System B data (target)
target = spark.createDataFrame([
    (1, "Product A", 100),
    (2, "Product B", 250),  # Price difference!
    (4, "Product D", 400)   # New product!
], ["id", "name", "price"])

# TODO 5a: Find exact matches between source and target
matches = source.intersect(target)
matches.show()

# TODO 5b: Find records in source but not in target (or different)
source_only = target.subtract(source)
source_only.show()

# TODO 5c: Find records in target but not in source (or different)
target_only = target.subtract(source)
target_only.show()

# TODO 5d: Create a reconciliation report showing:
# - Matched count
# - Source-only count
# - Target-only count
matched_count = matches.count()
source_only_count = source_only.count()
target_only_count = target_only.count()

print("Reconciliation Report:")
print("matched: ", matched_count)
print("source only:", source_only_count)
print("target only:", target_only_count)


# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()