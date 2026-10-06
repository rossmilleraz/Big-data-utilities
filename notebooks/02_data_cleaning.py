# This file was created and run in Databricks.
# It may not run locally

# Import the functions I'll need
from pyspark.sql.functions import col, when, year, month, date_format, hour


# Load the raw smart meter data
df = spark.read.csv(
    "/Workspace/Users/rossmilleraz@gmail.com/LCL-June2015v2_0.csv",
    header=True,
    inferSchema=True
)


# Clean up the column names and look at the data
df = df.toDF(*[c.strip() for c in df.columns])

display(df.limit(10))


# Clean the energy column
# Change "Null" to actual null values and the rest to numbers
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


# Split DateTime into columns that will be easier to analyze later
df = (
    df
    .withColumn("year", year("DateTime"))
    .withColumn("month", month("DateTime"))
    .withColumn("day_of_week", date_format("DateTime", "EEEE"))
    .withColumn("hour", hour("DateTime"))
)


# Take a look at the cleaned data
display(df.limit(10))


# Make sure the columns and data types look right
df.printSchema()


# See how many rows are left after cleaning
print("Clean rows:", df.count())


# Save the cleaned data so I can use it in the next notebook
df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_smart_meter")
