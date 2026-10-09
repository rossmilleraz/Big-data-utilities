# This file was created and run in Databricks.
# It may not run locally because Databricks provides the Spark environment automatically.

# Imports for cleaning the data
from pyspark.sql.functions import col, when, year, month, date_format, hour


# Load the smart meter CSV files
# The * will let me load multiple files once I upload more
df = spark.read.csv(
    "/Workspace/Users/rossmilleraz@gmail.com/LCL-June2015v2_*.csv",
    header=True,
    inferSchema=True
)


# Check how many rows were loaded
print("Total rows:", df.count())


# Take a look at the data
display(df.limit(10))

# Check the columns and data types
df.printSchema()


# Remove extra spaces from column names
df = df.toDF(*[c.strip() for c in df.columns])


# Change the energy readings to numbers
# Replace "Null" with actual null values
df = (
    df
    .withColumn(
        "energy_kwh",
        when(
            col("KWH/hh (per half hour)") == "Null",
            None
        ).otherwise(
            col("KWH/hh (per half hour)").cast("double")
        )
    )
    .drop("KWH/hh (per half hour)")
)


# Remove rows with missing energy readings
df = df.dropna(subset=["energy_kwh"])


# Add columns to make the dates easier to work with
df = (
    df
    .withColumn("year", year("DateTime"))
    .withColumn("month", month("DateTime"))
    .withColumn("day_of_week", date_format("DateTime", "EEEE"))
    .withColumn("hour", hour("DateTime"))
)


# Make sure the cleaned data looks right
display(df.limit(10))


# Check how many rows are left
print("Clean rows:", df.count())


# Save the cleaned data as a new Delta table
df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("smart_meter_full")


# Compare the original sample with the larger dataset
sample_rows = spark.table("silver_smart_meter").count()
full_rows = spark.table("smart_meter_full").count()

print("Original sample:", sample_rows)
print("Larger dataset:", full_rows)