from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("RetailInventory").enableHiveSupport().getOrCreate()

# Define schemas
transactions_schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("city_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price_usd", DoubleType(), True),
    StructField("discount_percent", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("customer_age", IntegerType(), True),
    StructField("transaction_date", StringType(), True)
])

items_schema = StructType([
    StructField("item_id", IntegerType(), True),
    StructField("item_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("supplier", StringType(), True),
    StructField("price_usd", DoubleType(), True),
    StructField("weight_kg", DoubleType(), True),
    StructField("color", StringType(), True),
    StructField("warranty_years", IntegerType(), True),
    StructField("release_year", IntegerType(), True),
    StructField("rating", DoubleType(), True)
])

cities_schema = StructType([
    StructField("city_id", IntegerType(), True),
    StructField("city_name", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("population", IntegerType(), True),
    StructField("area_sq_km", DoubleType(), True),
    StructField("average_income_usd", IntegerType(), True),
    StructField("founded_year", IntegerType(), True),
    StructField("time_zone", StringType(), True),
    StructField("climate", StringType(), True)
])

# Load CSVs
transactions_df = spark.read.csv("s3://retail-inventory-capstone-bucket/Bronze/Transaction_raw_data/transactions.csv",
    schema=transactions_schema,
    header=True,
    sep=","
)

items_df = spark.read.csv(
    "s3://retail-inventory-capstone-bucket/Bronze/items.csv",
    schema=items_schema,
    header=True,
    sep=","
)

cities_df = spark.read.json(
    "s3://retail-inventory-capstone-bucket/Bronze/cities.json",
    schema=cities_schema,
    multiLine=True
)

print("===== Transactions Schema =====")
transactions_df.printSchema()
transactions_df.describe().show()

print("===== Items Schema =====")
items_df.printSchema()
items_df.describe().show()

print("===== Cities Schema =====")
cities_df.printSchema()
cities_df.describe().show()

## CLEANING ITEMS DF

from pyspark.sql.functions import *

print("Total records before cleaning:", items_df.count())
# Check nulls in items
items_df.select([count(when(col(c).isNull(), c)).alias(c) for c in items_df.columns]).show()


import datetime

# Removing duplicate item_id
cleaned_items_df = items_df.dropDuplicates(["item_id"])

# Drop rows with nulls in critical columns
required_columns = ["item_id", "item_name", "price_usd", "weight_kg", "category", "release_year"]
cleaned_items_df = cleaned_items_df.dropna(subset=required_columns)

# Filter invalid prices and weights
cleaned_items_df = cleaned_items_df.filter((col("price_usd") > 0) & (col("weight_kg") > 0))

# Filter warranty (valid in [0-5])
cleaned_items_df = cleaned_items_df.filter((col("warranty_years") >= 0) & (col("warranty_years") <= 5))

# Filter based on acceptable release years (2000 to current year)
current_year = datetime.datetime.now().year
cleaned_items_df = cleaned_items_df.filter((col("release_year") >= 2000) & (col("release_year") <= current_year))

# Add derived column: item_age
cleaned_items_df = cleaned_items_df.withColumn("item_age", lit(current_year) - col("release_year"))

print("Total records after cleaning:", cleaned_items_df.count())
cleaned_items_df.describe(["price_usd", "weight_kg", "item_age", "rating"]).show()

cleaned_items_df.write.mode("overwrite").option("header", "true").csv("s3://retail-inventory-capstone-bucket/Silver/cleaned_items/")

print('Total Records before cleaning: ', cities_df.count())

## CLEANING CITIES DF

from pyspark.sql.functions import *

cleaned_df=cities_df

# Remove duplicate rows based on 'city_id'
cleaned_df = cleaned_df.dropDuplicates(['city_id'])

# Filling missing values in 'average_income_usd' with the mean
# Calculate the mean of 'average_income_usd' (excluding nulls).
mean_income = cleaned_df.agg(mean('average_income_usd')).first()[0]

cleaned_df = cleaned_df.fillna({'average_income_usd': mean_income})

# Remove rows where the 'population' is negative
cleaned_df = cleaned_df.filter(col('population') > 0)

# Standardize 'city_name' by removing leading/trailing spaces
cleaned_df = cleaned_df.withColumn('city_name', trim(col('city_name')))

# Filter cities by 'founded_year' to keep only reasonable years (between 1000 and 2025)
cleaned_df = cleaned_df.filter((col('founded_year') >= 1000) & (col('founded_year') <= 2025))

# Remove rows where 'state' or 'country' is missing (null)
cleaned_df = cleaned_df.filter(col('state').isNotNull() & col('country').isNotNull())

## OUTLIERS CODE

def clean_outliers(df, cols):
    """
    Remove outliers from specified columns using the IQR (Interquartile Range) method.
    """
    for c in cols:
        # Compute Q1 and Q3 for the column
        quantiles = df.approxQuantile(c, [0.25, 0.75], 0.05)
        Q1, Q3 = quantiles
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        # Filter out rows outside the IQR bounds
        df = df.filter((col(c) >= lower_bound) & (col(c) <= upper_bound))
    return df

df_clean_no_outliers = clean_outliers(df_clean, ['quantity', 'price_usd'])


df_clean_no_outliers.write.mode("overwrite").option("header", "true").csv(output_path)

df_clean_no_outliers.write.mode("overwrite").partitionBy("year", "month").format("parquet").save("s3://retail-inventory-capstone-bucket/Gold/transactions/")

# Confirmation
print(f"✅ Cleaned transactions data written to: {output_path}")


## READING FROM GOLD FOLDER

transactions_gold_df = df_gold
cities_gold_df = spark.read.option("header", "true").csv("s3://retail-inventory-capstone-bucket/Gold/cities/")
items_gold_df = spark.read.option("basePath", "s3://retail-inventory-capstone-bucket/Gold/items/").parquet("s3://retail-inventory-capstone-bucket/Gold/items/")


cleaned_items_df.write.mode('overwrite').partitionBy("category").format('parquet').save("s3://retail-inventory-capstone-bucket/Gold/items/")

## BUSINESS QUERIES

#  1. Top-selling Items by Quantity
top_items = transactions_gold_df.groupBy("item_id").sum("quantity").orderBy(col("sum(quantity)").desc())
top_items.show()


from pyspark.sql.functions import date_format, to_date, col, sum
#  2. Revenue by Item Category
transactions_gold_df = transactions_gold_df.withColumnRenamed("price_usd", "price")
transactions_items = transactions_gold_df.join(items_gold_df, "item_id")
category_revenue = transactions_items.groupBy("category").sum("price").orderBy(col("sum(price)").desc())
category_revenue.show()


#  3. Monthly Revenue Trend
transactions_gold_df = transactions_gold_df.withColumn("month", date_format(to_date("transaction_date"), "yyyy-MM"))
monthly_revenue = transactions_gold_df.groupBy("month").sum("price").orderBy("month")
monthly_revenue.show()

#  4. Top Cities by Revenue
transactions_cities = transactions_gold_df.join(cities_gold_df, "city_id")
revenue_by_city = transactions_cities.groupBy("city_name").sum("price").orderBy(col("sum(price)").desc())
revenue_by_city.show()



#  5. Most Popular Payment Methods
payment_methods = transactions_gold_df.groupBy("payment_method").count().orderBy(col("count").desc())
payment_methods.show()


#  6. Average Order Value by City
avg_order_by_city = transactions_gold_df.groupBy("city_id").avg("price").orderBy(col("avg(price)").desc())
avg_order_by_city.show()


#  7. Discount Impact on Sales
discount_impact = transactions_gold_df.withColumn("discounted", col("discount_percent") > 0).groupBy("discounted").sum("quantity")
discount_impact.show()


#  8. Age Demographics of Buyers
age_distribution = transactions_gold_df.groupBy("customer_age").count().orderBy("customer_age")
age_distribution.show()


#  9. Most Effective Item Discounts
item_discounts = transactions_gold_df.groupBy("item_id").avg("discount_percent").orderBy(col("avg(discount_percent)").desc())
item_discounts.show()



#  10. Revenue Trend by Category Over Time
trend_category = transactions_items.withColumn("month", date_format(to_date("transaction_date"), "yyyy-MM")) \
    .groupBy("month", "category").agg(sum("price_usd").alias("total_sum")).orderBy("month", "category")
trend_category.show()


#  11. Revenue per Payment Method
revenue_per_payment = transactions_gold_df.groupBy("payment_method").sum("price").orderBy(col("sum(price)").desc())
revenue_per_payment.show()


#  12. City Performance: Revenue vs. Population
city_rev_pop = transactions_cities.groupBy("city_name", "population").sum("price").orderBy("population")
city_rev_pop.show()


#  13. Average Price by Item Category
avg_price_category = transactions_items.groupBy("category").avg("price").orderBy(col("avg(price)").desc())
avg_price_category.show()



#  14. Items with Highest Revenue per Unit Sold
rev_per_unit = transactions_gold_df.groupBy("item_id").agg((sum("price")/sum("quantity")).alias("rev_per_unit")) \
    .orderBy(col("rev_per_unit").desc())
rev_per_unit.show()


# 15. Correlation Between City Population and Total Revenue using broadcast join
# Broadcast cities and join to transactions
bcast_cities = broadcast(cities_gold_df)
pop_revenue = transactions_gold_df.join(bcast_cities, "city_id").groupBy("population").sum("price").orderBy("population")
pop_revenue.show()


# 1. Broadcast join: Top revenue items with item details
top_revenue_items = transactions_gold_df.groupBy("item_id").sum("price").alias("total_revenue")

top_items_with_details = top_revenue_items.join(
    broadcast(items_gold_df), "item_id"
).select("item_id", "category", "supplier", col("sum(price)").alias("total_revenue")).orderBy(col("total_revenue").desc())

top_items_with_details.show()

top_items_with_details.write.mode("overwrite").option("header", "true").csv("s3://retail-inventory-capstone-bucket/quicksight/top-items/")


# 2. Broadcast join: Payment method counts by city region
from pyspark.sql.functions import broadcast, col
payment_city_region = transactions_gold_df.join(
    broadcast(cities_gold_df.select("city_id", "state")), "city_id"
).groupBy("state", "payment_method").count().orderBy("state", col("count").desc())

payment_city_region.show()
payment_city_region.write.mode("overwrite").option("header", "true").csv("s3://retail-inventory-capstone-bucket/quicksight/payment-city-region/")


# 3. Broadcast join: Average discount percent by item supplier
avg_discount_supplier = transactions_gold_df.join(
    broadcast(items_gold_df.select("item_id", "supplier")), "item_id"
).groupBy("supplier").avg("discount_percent").orderBy(col("avg(discount_percent)").desc())

avg_discount_supplier.show()
avg_discount_supplier.write.mode("overwrite").option("header", "true").csv("s3://retail-inventory-capstone-bucket/quicksight/avg-discount-supplier/")


# 4. Broadcast join + filter: Revenue by category for cities with population > 1 million
filtered_cities = cities_gold_df.filter(col("population") > 1_000_000)

joined_df = transactions_gold_df.join(broadcast(items_gold_df.select("item_id", "category")), "item_id").join(filtered_cities.select("city_id", "city_name", "population"), "city_id")

joined_df.groupBy("category", "city_name").sum("price_usd").orderBy(col("sum(price_usd)").desc()).show()

joined_df.write.mode("overwrite").option("header", "true").csv("s3://retail-inventory-capstone-bucket/quicksight/revenue-category-city/")


# 5. Broadcast join: Count transactions by climate type
transaction_climate_count = transactions_gold_df.join(
    broadcast(cities_gold_df.select("city_id", "climate")), "city_id"
).groupBy("climate").count().orderBy(col("count").desc())

transaction_climate_count.show()
transaction_climate_count.write.mode("overwrite").option("header", "true").csv("s3://retail-inventory-capstone-bucket/quicksight/transaction-climate-count/")
