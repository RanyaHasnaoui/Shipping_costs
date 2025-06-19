from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
from dotenv import load_dotenv
import os
# Create Spark session
spark = SparkSession.builder \
    .appName("Clean All Sources") \
    .config("spark.jars", "/opt/spark/jars/mysql-connector-java-8.0.33.jar") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# MySQL connection parameters
# Load variables from .env
load_dotenv()

mysql_host = os.getenv("MYSQL_HOST")
mysql_port = os.getenv("MYSQL_PORT")
mysql_db = os.getenv("MYSQL_DB")
mysql_user = os.getenv("MYSQL_USER")
mysql_password = os.getenv("MYSQL_PASSWORD")

mysql_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_db}"
mysql_properties = {
    "user": mysql_user,
    "password": mysql_password,
    "driver": "com.mysql.cj.jdbc.Driver"
}
# -------------------- DIM_DATE --------------------
start_date = "2025-01-01"
end_date = "2025-12-31"

start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())

dim_date = spark.range(start_ts, end_ts + 1, 86400) \
    .withColumn("date", from_unixtime(col("id")).cast("date")) \
    .withColumn("date_key", col("date")) \
    .drop("id") \
    .withColumn("year", year("date")) \
    .withColumn("month", month("date")) \
    .withColumn("day", dayofmonth("date")) \
    .withColumn("day_of_week", date_format("date", "EEEE")) \
    .withColumn("month_name", date_format("date", "MMMM")) \
    .withColumn("quarter", quarter("date")) \
    .withColumn("is_weekend", when(date_format("date", "u").cast("int") >= 6, 1).otherwise(0)) \
    .select("date_key", "year", "month", "day", "day_of_week", "month_name", "quarter", "is_weekend")

dim_date.write.jdbc(url=mysql_url, table="dim_date", mode="overwrite", properties=mysql_properties)

# -------------------- DIM_WEATHER --------------------
weather_df = spark.read.option("multiLine", True).json("/tmp/source1.json")

# Flatten and process weather data
hourly_df = weather_df.select(explode("hours").alias("hour"))
wind_sources = hourly_df.select("hour.windSpeed.*").schema.names

cleaned_weather = hourly_df.withColumn("wind_array", array(*[
    col(f"hour.windSpeed.{src}") for src in wind_sources
])).select(
    to_timestamp("hour.time").alias("time"),
    to_date("hour.time").alias("date"),
    round(
        expr("aggregate(filter(wind_array, x -> x is not null), 0D, (acc, x) -> acc + x) / size(filter(wind_array, x -> x is not null))"),
        2
    ).alias("avg_wind_speed")
).dropna()


cleaned_weather.select("time", "avg_wind_speed", "date") \
    .write.jdbc(url=mysql_url, table="dim_weather", mode="append", properties=mysql_properties)

# -------------------- DIM_BRENT_PRICES --------------------
crude_df = spark.read.json("/tmp/source2.json")

dim_brent_prices = crude_df.selectExpr("explode(response.data) as row") \
    .selectExpr("row.period as period", "CAST(row.value AS DOUBLE) as value") \
    .dropna() \
    .withColumn("period", to_date("period", "yyyy-MM-dd"))

dim_brent_prices.write.jdbc(url=mysql_url, table="dim_brent_prices", mode="append", properties=mysql_properties)

# -------------------- DIM_CURRENCY_RATES --------------------
currency_df = spark.read.json("/tmp/source3.json")

dim_currency = currency_df.selectExpr(
    "conversion_rates.USD as usd_rate",
    "conversion_rates.EUR as eur_rate",
    "time_last_update_utc"
).dropna() \
    .withColumn("base_code", lit("USD")) \
    .withColumn("time_last_update_utc", to_timestamp("time_last_update_utc", "EEE, dd MMM yyyy HH:mm:ss Z")) \
    .select("base_code", "usd_rate", "eur_rate", "time_last_update_utc")

dim_currency.write.jdbc(url=mysql_url, table="dim_currency_rates", mode="append", properties=mysql_properties)

# -------------------- FACT TABLE --------------------

# Read dimension tables
dim_date = spark.read.jdbc(url=mysql_url, table="dim_date", properties=mysql_properties)
dim_weather = spark.read.jdbc(url=mysql_url, table="dim_weather", properties=mysql_properties)
dim_brent = spark.read.jdbc(url=mysql_url, table="dim_brent_prices", properties=mysql_properties)
dim_currency = spark.read.jdbc(url=mysql_url, table="dim_currency_rates", properties=mysql_properties)

# Get daily aggregates
daily_weather = dim_weather.groupBy("date").agg(
    avg("avg_wind_speed").alias("avg_wind_speed"),
    first("weather_key").alias("weather_key")
)

# Get most recent currency rate per day
daily_currency = dim_currency.groupBy(to_date("time_last_update_utc").alias("update_date")).agg(
    first("usd_rate").alias("usd_rate"),
    first("currency_key").alias("currency_key")
)

# Create fact table with exact column names and types
fact_df = dim_date.join(
    daily_weather,
    dim_date["date_key"] == daily_weather["date"],
    "left"
).join(
    dim_brent,
    dim_date["date_key"] == dim_brent["period"],
    "left"
).join(
    daily_currency,
    dim_date["date_key"] == daily_currency["update_date"],
    "left"
).select(
    dim_date["date_key"].alias("date"),
    coalesce(col("weather_key"), lit(-1)).alias("weather_key"),
    coalesce(col("brent_key"), lit(-1)).alias("brent_key"),
    coalesce(col("currency_key"), lit(-1)).alias("currency_key"),
    
    # Calculations
    coalesce(
        round(col("value") * col("usd_rate") + (col("avg_wind_speed") * 5), 2),
        round(col("avg_wind_speed") * 5, 2),
        lit(0.0)
    ).cast("double").alias("total_cost_usd"),
    
    coalesce(round(col("value"), 2), lit(0.0)).cast("double").alias("brent_cost_usd"),
    
    when(col("avg_wind_speed") > 20, 3)
    .when(col("avg_wind_speed") > 10, 2)
    .otherwise(1).cast("integer").alias("weather_impact_score"),
    
    when(col("value") > 100, "High")
    .when(col("value") > 50, "Medium")
    .otherwise("Low").cast("string").alias("fuel_cost_category"),
    
    coalesce(round(col("usd_rate"), 4), lit(1.0)).cast("double").alias("currency_exchange_rate")
).distinct()

# Write to MySQL
fact_df.write.jdbc(
    url=mysql_url,
    table="fact_shipping_costs",
    mode="overwrite",
    properties=mysql_properties
)

spark.stop()