from pyspark.sql import SparkSession
from pyspark_diff_tool.comparison import compare_dataframes

spark = SparkSession.builder.appName("ComparisonExample").getOrCreate()

# Sample DataFrame 1
data1 = [
    (1, "Alice", 90.0),
    (2, "Bob", 85.5),
    (3, "Charlie", 78.0)
]
columns1 = ["id", "name", "score"]
df1 = spark.createDataFrame(data1, columns1)

# Sample DataFrame 2 (with a small change)
data2 = [
    (1, "Alice", 90.0),
    (2, "Bob", 86.0),
    (4, "David", 70.0)
]
columns2 = ["id", "name", "score"]
df2 = spark.createDataFrame(data2, columns2)

# Run comparison
mismatched_df, summary_df, drop_coverage, add_coverage = compare_dataframes(
    df1, df2, key="id", threshold=0.5, local_test_cases=False
)

# Display results
print("Mismatches:")
mismatched_df.show(truncate=False)

print("Summary:")
summary_df.show(truncate=False)

print(f"Dropped Keys: {drop_coverage[0]:.2f}%")
drop_coverage[1].show(truncate=False)

print(f"Added Keys: {add_coverage[0]:.2f}%")
add_coverage[1].show(truncate=False)