from pyspark.sql import SparkSession
from pyspark.sql.functions import col , isnull, year, month, when


spark = SparkSession.builder.appName("de_spark").getOrCreate()


data = spark.read.csv("s3://auto-de-data/raw-data/city_market_tracker.tsv000.gz", header=True, inferSchema=True, sep= "\t")


important_columns = [
    "period_begin",
    "period_end",
    "region",
    "city",
    "state",
    "property_type",
    "median_sale_price",
    "median_list_price",
    "homes_sold",
    "pending_sales",
    "new_listings",
    "inventory",
    "months_of_supply",
    "median_dom",
    "avg_sale_to_list",
    "sold_above_list",
    "price_drops",
    "last_updated"
]

df = data.select(important_columns)


df = df.na.drop()


#Extract year 
df = df.withColumn("period_end_yr", year(col("period_end")))

#Extract month
df = df.withColumn("period_end_month", month(col("period_end")))


df = df.withColumn("period_end_month", 
                   when(col("period_end_month") == 1, "January")
                   .when(col("period_end_month") == 2, "February")
                   .when(col("period_end_month") == 3, "March")
                   .when(col("period_end_month") == 4, "April")
                   .when(col("period_end_month") == 5, "May")
                   .when(col("period_end_month") == 6, "June")
                   .when(col("period_end_month") == 7, "July")
                   .when(col("period_end_month") == 8, "August")
                   .when(col("period_end_month") == 9, "September")
                   .when(col("period_end_month") == 10, "October")
                   .when(col("period_end_month") == 11, "November")
                   .when(col("period_end_month") == 12, "December")
                   .otherwise("Unknown")
                 )


df = df.drop("period_end", "period_begin") #removing columns not needed upon further analysis


# Now we can save this data in another S3 bucket in parquet format since it is a better way to save compared to csv for large size

#df.write.mode("overwrite").csv("s3://auto-de-data/transform-data/cleaned_data.csv")

df.write.mode("overwrite").csv("s3://auto-de-data/transform-data/cleaned_data.csv")



