"""
Exercise: Column Management
===========================
Week 2, Wednesday

Practice adding, removing, and transforming columns on product inventory data.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, upper, lower, trim, concat, concat_ws,
    split, substring, regexp_replace, coalesce, current_date, initcap
)

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: Columns").master("local[*]").getOrCreate()

# Product inventory data (messy data for cleaning!)
inventory = spark.createDataFrame([
    (1, "  LAPTOP pro  ", "Electronics", 999.99, 50, None),
    (2, "  phone X ", "Electronics", 799.99, 100, "NY"),
    (3, "Winter JACKET", "Clothing", 149.99, 200, "CA"),
    (4, " running shoes ", "Clothing", 89.99, None, "TX"),
    (5, "coffee MAKER", "Home", 49.99, 75, None),
    (6, "  Desk Lamp  ", "Home", 29.99, 120, "NY")
], ["product_id", "product_name", "category", "price", "quantity", "warehouse"])

print("=== Exercise: Column Management ===")
print("\nRaw Inventory Data:")
inventory.show(truncate=False)

# =============================================================================
# TASK 1: String Cleaning (20 mins)
# =============================================================================

print("\n--- Task 1: String Cleaning ---")

# TODO 1a: Clean product_name: trim whitespace, convert to title case
# HINT: trim() removes whitespace, initcap() for title case
inventory = inventory.withColumn(
    "product_name", 
    initcap(trim(col("product_name")))
)
# inventory.show()

# TODO 1b: Standardize category to lowercase
inventory = inventory.withColumn(
    "category",
    lower(col("category"))
)
#inventory.show()

# TODO 1c: Create a "product_code" column by:
# - Taking first 3 letters of category (uppercase)
# - Adding the product_id
# - Example: "ELE-1" for Electronics product 1
inventory = inventory.withColumn(
    "product_code",
    concat(
        upper(substring(col("category"), 1, 3)),
        lit("_"),
        col("product_id")
    )
)

inventory.show()


# =============================================================================
# TASK 2: Handling Nulls (15 mins)
# =============================================================================

print("\n--- Task 2: Handling Nulls ---")

# TODO 2a: Replace null warehouse with "CENTRAL"
inventory = inventory.withColumn(
    "warehouse",
    coalesce(col("warehouse"), lit("CENTRAL"))
)
# inventory.show()
# TODO 2b: Replace null quantity with 0
inventory = inventory.withColumn(
    "quantity",
    coalesce(col("quantity"), lit(0))
)
#inventory.show()
# TODO 2c: Create an "in_stock" boolean column (quantity > 0 or not null)
inventory = inventory.withColumn(
    "in_stock",
    col("quantity") > 0
)
inventory.show()

# =============================================================================
# TASK 3: Calculated Columns (20 mins)
# =============================================================================

print("\n--- Task 3: Calculated Columns ---")

# TODO 3a: Add "inventory_value" = price * quantity (handle nulls!)
inventory = inventory.withColumn(
    "inventory_value",
    col("price") * col ("quantity")

)
# inventory.show()

# TODO 3b: Add "price_tier" based on price:
# - "Budget" if price < 50
# - "Mid" if 50 <= price < 200
# - "Premium" if price >= 200
inventory = inventory.withColumn(
    "price_tier",
    when(col("price") < 50, "Budget")
    .when((col("price") >=50) & (col("price") <200), "MId")
    .otherwise("Premium")
)

# TODO 3c: Add "last_updated" column with today's date
inventory = inventory.withColumn(
    "last_updated",
    current_date()
)
inventory.show()

# =============================================================================
# TASK 4: Removing and Renaming (10 mins)
# =============================================================================

print("\n--- Task 4: Removing and Renaming ---")

# TODO 4a: Drop the "warehouse" column
inventory = inventory.drop("warehouse")
# inventory.show()


# TODO 4b: Rename columns:
# - product_id -> id
# - product_name -> name
inventory = (
    inventory.withColumnRenamed("product_id", "id")
    .withColumnRenamed("product_name", "name")
)
inventory.show()

# =============================================================================
# TASK 5: Complete Data Pipeline (25 mins)
# =============================================================================

print("\n--- Task 5: Complete Data Pipeline ---")

# Create a clean, analysis-ready version of the data:
clean_inventory = spark.createDataFrame([
    (1, "  LAPTOP pro  ", "Electronics", 999.99, 50, None),
    (2, "  phone X ", "Electronics", 799.99, 100, "NY"),
    (3, "Winter JACKET", "Clothing", 149.99, 200, "CA"),
    (4, " running shoes ", "Clothing", 89.99, None, "TX"),
    (5, "coffee MAKER", "Home", 49.99, 75, None),
    (6, "  Desk Lamp  ", "Home", 29.99, 120, "NY")
], ["product_id", "product_name", "category", "price", "quantity", "warehouse"])
# 1. Clean product_name (trim, title case)
clean_inventory = clean_inventory.withColumn("product_name", initcap(trim(col("product_name"))))
# clean_inventory.show()
# 2. Fill null warehouse with "CENTRAL"
clean_inventory= clean_inventory.withColumn("warehouse", coalesce(col("warehouse"), lit("CENTRAL")))
# clean_inventory.show()
# 3. Fill null quantity with 0
clean_inventory=clean_inventory.withColumn("quantity", coalesce(col("quantity"), lit(0)))
#clean_inventory.show()
# 4. Add inventory_value column
clean_inventory = clean_inventory.withColumn("inventory_value", col("price") * col("quantity"))
#clean_inventory.show()

# 5. Add price_tier column
clean_inventory = clean_inventory.withColumn("price_tier",
                                    when(col("price") <50, "budget")
                                    .when((col("price") >=50) & (col("price") <200), "Mid")
                                    .otherwise("Premium")
                                             )
#clean_inventory.show()
# 6. Add last_updated column
clean_inventory = clean_inventory.withColumn("last_updated", current_date())
#clean_inventory.show()
# 7. Rename product_id to id, product_name to name
clean_inventory = clean_inventory.withColumnRenamed("product_id", "id")
clean_inventory = clean_inventory.withColumnRenamed("product_name", "name")

# 8. Drop warehouse column
clean_inventory = clean_inventory.drop("warehouse")

# 9. Order columns: id, name, category, price, quantity, inventory_value, price_tier, last_updated
clean_inventory = clean_inventory.select(
        "id","name","category","price","quantity","inventory_value","price_tier","last_updated"
    )

clean_inventory.show()


# =============================================================================
# CHALLENGE: Extract and Parse (15 mins)
# =============================================================================

print("\n--- Challenge: String Parsing ---")

# Product descriptions
descriptions = spark.createDataFrame([
    ("Widget A - Size: Large, Color: Blue",),
    ("Gadget B - Size: Medium, Color: Red",),
    ("Tool C - Size: Small, Color: Green",)
], ["description"])

# TODO 6a: Extract just the product name (before the dash)
descriptions = descriptions.withColumn(
    "product_name",
    trim(split(col("description"), "-").getItem(0))
)

# TODO 6b: Extract the size value
descriptions = descriptions.withColumn(
    "size",
    regexp_replace(
        split(col("description"), "Size: ").getItem(1), ",.*", ""
    )
)

# TODO 6c: Extract the color value
descriptions = descriptions.withColumn(
    "color",
    regexp_replace(
        split(col("description"), "Color: ").getItem(1), "",""
    )
)
descriptions.show(truncate=False)

# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()