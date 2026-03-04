"""
Exercise: Datasets and Type Safety
==================================
Week 2, Wednesday

Explore the Dataset/DataFrame paradigm and type-aware patterns in PySpark.
"""

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, lit

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: Datasets").master("local[*]").getOrCreate()

print("=== Exercise: Datasets and Type Safety ===")

# =============================================================================
# TASK 1: Understanding Row Objects (15 mins)
# =============================================================================

print("\n--- Task 1: Row Objects ---")

# TODO 1a: Create 3 Row objects for employees
# Each should have: name (string), age (int), department (string)
employee1 = Row(name="Alice", age=30, department="Engineering")  # Row(name=..., age=..., department=...)
employee2 = Row(name="Bob", age=25, department="Marketing")
employee3 = Row(name="Charlie", age=35, department="Sales")


# TODO 1b: Create a DataFrame from these Row objects
employees_df = spark.createDataFrame([employee1, employee2, employee3])
employees_df.show()

# TODO 1c: Access data from the first row using:
# - Attribute access (row.name)
# - Index access (row[0])
# - Key access (row["name"])
first_row = employees_df.first()
print("Attribute access:", first_row.name)
print("Index access:", first_row[0])
print("Key access: ", first_row["name"])

# TODO 1d: Convert a Row to a dictionary
row_dict = first_row.asDict()
print("Row as dict:", row_dict)

# =============================================================================
# TASK 2: Explicit Schemas (20 mins)
# =============================================================================

print("\n--- Task 2: Explicit Schemas ---")

# TODO 2a: Define a schema for a Product type with:
# - id (IntegerType, not nullable)
# - name (StringType, nullable)
# - price (DoubleType, nullable)
# - category (StringType, nullable)

product_schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("price", DoubleType(), nullable=True),
    StructField("category", StringType(), nullable=True)
])


# TODO 2b: Create sample product data and create DataFrame with the schema
products_data = [
    (1, "Laptop", 999.99, "Electronics"),
    (2, "Coffee Maker", 49.99, "Home"),
    (3, "Running Shoes", None, "Sports")  # Note: null price
]
products_df=spark.createDataFrame(products_data, schema=product_schema)
products_df.show()


# TODO 2c: Verify the schema matches your definition
products_df.printSchema()

# TODO 2d: What happens if you try to create data that violates the schema?
# Try creating data with wrong types and observe the error
# (Comment out after testing to avoid breaking the script)


# =============================================================================
# TASK 3: Schema Validation (20 mins)
# =============================================================================

print("\n--- Task 3: Schema Validation ---")

# Incoming data that needs validation
incoming_data = spark.createDataFrame([
    (1, "Alice", 30, "Engineering"),
    (2, None, 25, "Marketing"),      # Null name - invalid?
    (3, "Charlie", -5, "Sales"),     # Negative age - invalid!
    (4, "Diana", 150, "Engineering") # Age too high - invalid!
], ["id", "name", "age", "department"])

print("Incoming data:")
incoming_data.show()

# TODO 3a: Create a validation function that checks:
# - name is not null
# - age is between 18 and 100
# Return two DataFrames: valid and invalid

def validate_employees(df):
    """
    Validates employee data.
    Returns: (valid_df, invalid_df)
    """
    valid = df.filter((col("name").isNotNull()) & (col("age") >= 18) & (col("age") <= 100))
    invalid = df.exceptAll(valid)
    return valid, invalid


# TODO 3b: Apply validation and show results
valid_df, invalid_df = validate_employees(incoming_data)
print("Valid employees:")
valid_df.show()
print("Invalid employees:")
invalid_df.show()


# =============================================================================
# TASK 4: Working with RDD for Typed Transformations (20 mins)
# =============================================================================

print("\n--- Task 4: RDD Typed Transformations ---")

# Sample DataFrame
people = spark.createDataFrame([
    ("Alice", 30),
    ("Bob", 25),
    ("Charlie", 35)
], ["name", "age"])

# TODO 4a: Convert DataFrame to RDD
people_rdd = people.rdd

# TODO 4b: Use map() to transform each row, adding a category:
# - "Young" if age < 30
# - "Senior" if age >= 30
# Create new Row objects in the map function
def categorize(row):
    category = "Young" if row.age < 30 else "Senior"
    return Row(name=row.name, age=row.age, category = category)

people_rdd2 = people_rdd.map(categorize)
# TODO 4c: Convert the transformed RDD back to DataFrame
people_categorized_df=spark.createDataFrame(people_rdd2)
people_categorized_df.show()

# TODO 4d: Why might you use RDD transformations instead of DataFrame?
# Answer in a comment:RDD transformation are useful when you need control
#over elements.


# =============================================================================
# TASK 5: Schema Evolution (15 mins)
# =============================================================================

print("\n--- Task 5: Schema Evolution ---")

# Version 1 data
v1_data = spark.createDataFrame([
    (1, "Widget A", 19.99),
    (2, "Widget B", 29.99)
], ["id", "name", "price"])

# Version 2 data (has additional column)
v2_data = spark.createDataFrame([
    (3, "Widget C", 39.99, "Electronics"),
    (4, "Widget D", 49.99, "Home")
], ["id", "name", "price", "category"])

print("V1 schema:")
v1_data.printSchema()
print("V2 schema:")
v2_data.printSchema()

# TODO 5a: Evolve V1 data to match V2 schema
# Add the missing "category" column with null values
v1_evolved = v1_data.withColumn("category", lit(None).cast(StringType()))


# TODO 5b: Combine V1 and V2 data using unionByName
combined_df = v1_evolved.unionByName(v2_data)
combined_df.show()

# =============================================================================
# CONCEPTUAL QUESTIONS
# =============================================================================

print("\n--- Conceptual Questions ---")

# Answer in comments:

# Q1: In Scala/Java, what is the difference between Dataset[Row] and Dataset[Person]?
# ANSWER: Dataset[Row] is untyped, rows are generic, while Dataset[Person]
#is strongly typed. 
#

# Q2: Why does Python not have true typed Datasets like Scala?
# ANSWER: python is dynamically typed, pyspark cannot enforce compile-time type safety
#

# Q3: What are the benefits of using explicit schemas in production?
# ANSWER:ensure data consistency, allows validation, improve performance
#

# Q4: When would schema validation at the application level be important?
# ANSWER: when data come from unreliable sources, APIs or user inputs
#


# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()