# This file was created and run in Databricks.
# It may not run locally

# Import PySpark functions
from pyspark.sql.functions import (
    col, when, min, max, sum, avg, count,
    year, month, date_format, hour
)


# Load smart meter data
df = spark.read.csv(
    "/Workspace/Users/rossmilleraz@gmail.com/LCL-June2015v2_0.csv",
    header=True,
    inferSchema=True
)


# View schema
df.printSchema()


# View sample data
display(df.limit(10))


# Count rows
print("Total rows:", df.count())


# Remove spaces from column names
df = df.toDF(*[c.strip() for c in df.columns])


# View tariff types
df.select("stdorToU").distinct().show()


# View basic statistics
df.describe().show()


# Check date range
df.select(
    min("DateTime").alias("start_date"),
    max("DateTime").alias("end_date")
).show()


# Count invalid energy readings
df.filter(
    col("KWH/hh (per half hour)") == "Null"
).count()


# Create cleaned Silver dataframe
silver_df = (
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


# Check missing values
silver_df.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in silver_df.columns
]).show()


# Remove missing energy readings
silver_df = silver_df.dropna(subset=["energy_kwh"])


# Add date and time columns
silver_df = (
    silver_df
    .withColumn("year", year("DateTime"))
    .withColumn("month", month("DateTime"))
    .withColumn("day_of_week", date_format("DateTime", "EEEE"))
    .withColumn("hour", hour("DateTime"))
)


# View cleaned data
display(silver_df.limit(10))


# Count unique households
print(
    "Unique households:",
    silver_df.select("LCLid").distinct().count()
)


# View energy usage statistics
silver_df.select("energy_kwh").describe().show()


# Check energy usage percentiles
silver_df.selectExpr(
    """
    percentile_approx(
        energy_kwh,
        array(0.25, 0.5, 0.75, 0.95, 0.99)
    ) as percentiles
    """
).show(truncate=False)


# Check for negative energy readings
print(
    "Negative readings:",
    silver_df.filter(col("energy_kwh") < 0).count()
)


# View highest energy readings
silver_df.orderBy(
    col("energy_kwh").desc()
).show(10)


# Count readings for each household
household_counts = (
    silver_df
    .groupBy("LCLid")
    .count()
    .orderBy("count")
)

household_counts.show(20)


# Calculate average usage by hour
gold_hourly = (
    silver_df
    .groupBy("hour")
    .agg(
        avg("energy_kwh").alias("avg_energy_kwh"),
        count("*").alias("readings")
    )
    .orderBy("hour")
)

display(gold_hourly)


# Calculate average usage by day of week
gold_weekday = (
    silver_df
    .groupBy("day_of_week")
    .agg(
        avg("energy_kwh").alias("avg_energy_kwh")
    )
)

display(gold_weekday)


# Calculate average usage by month
gold_monthly = (
    silver_df
    .groupBy("year", "month")
    .agg(
        avg("energy_kwh").alias("avg_energy_kwh")
    )
    .orderBy("year", "month")
)

display(gold_monthly)


# Check customer tariff groups
silver_df.groupBy("stdorToU").count().show()


# Save cleaned data as a Delta table
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_smart_meter")
