# This file was created and run in Databricks.
# It may not run locally because Databricks provides the Spark environment automatically.

# Imports for the analysis
from pyspark.sql.functions import avg, sum, count, col


# Load the cleaned data
df = spark.table("silver_smart_meter")

# Make sure everything looks right
display(df.limit(10))


# Look at average usage by hour
hourly_usage = (
    df
    .groupBy("hour")
    .agg(avg("energy_kwh").alias("avg_energy_kwh"))
    .orderBy("hour")
)

display(hourly_usage)


# See which hours have the highest usage
highest_hours = hourly_usage.orderBy(
    col("avg_energy_kwh").desc()
)

display(highest_hours)


# Look at average usage for each day of the week
weekday_usage = (
    df
    .groupBy("day_of_week")
    .agg(avg("energy_kwh").alias("avg_energy_kwh"))
    .orderBy(col("avg_energy_kwh").desc())
)

display(weekday_usage)


# Look at how average usage changes by month
monthly_usage = (
    df
    .groupBy("year", "month")
    .agg(avg("energy_kwh").alias("avg_energy_kwh"))
    .orderBy("year", "month")
)

display(monthly_usage)


# Look at usage for each household
household_usage = (
    df
    .groupBy("LCLid")
    .agg(
        avg("energy_kwh").alias("avg_energy_kwh"),
        sum("energy_kwh").alias("total_energy_kwh"),
        count("*").alias("readings")
    )
)

display(household_usage)


# See which households used the most energy
highest_households = (
    household_usage
    .orderBy(col("total_energy_kwh").desc())
)

display(highest_households.limit(10))


# Get the average usage for the whole dataset
overall_average = df.agg(
    avg("energy_kwh").alias("avg_energy_kwh")
)

display(overall_average)