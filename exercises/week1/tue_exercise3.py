#create accumulators
from pyspark import SparkContext

sc = SparkContext("local[*]", "AccumulatorExercise")

# Create accumulator
record_counter = sc.accumulator(0)

# Sample data
data = sc.parallelize(range(1, 101))

# Count records using accumulator
def count_record(x):
    record_counter.add(1)
    return x

data.map(count_record).collect()

print(f"Records processed: {record_counter.value}")

records = sc.parallelize([
    "100,Alice,Engineering",
    "200,Bob,Sales",
    "INVALID_RECORD",
    "300,Charlie,Marketing",
    "",  # Empty record
    "400,Diana,Engineering",
    "BAD_DATA_HERE",
    "500,Eve,Sales"
])
# Accumulators for tracking
total_records = sc.accumulator(0)
valid_records = sc.accumulator(0)
invalid_records = sc.accumulator(0)

def validate_record(record):
    total_records.add(1)
    
    # Check if record has 3 comma-separated fields
    if record and len(record.split(",")) == 3:
        valid_records.add(1)
        return record
    else:
        invalid_records.add(1)
        return None

valid_data = records.map(validate_record).filter(lambda x: x is not None)
valid_data.collect()

print(f"Total records: {total_records.value}")
print(f"Valid records: {valid_records.value}")
print(f"Invalid records: {invalid_records.value}")
print(f"Error rate: {invalid_records.value / total_records.value * 100:.1f}%")

