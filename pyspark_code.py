# playstore_insights.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when
import itertools

# Create a Spark session
spark = SparkSession.builder.appName("PlaystoreInsights").getOrCreate()

# Load the CSV data into a Spark DataFrame
data_file_path = "playstore.csv"
df = spark.read.option("header", "true").csv(data_file_path)

# Convert relevant columns to appropriate types
df = df.withColumn("releasedDayYear", col("releasedDayYear").cast("int"))
df = df.withColumn("price", col("price").cast("float"))
df = df.withColumn("ratings", col("ratings").cast("float"))
df = df.withColumn("minInstalls", col("minInstalls").cast("int"))

# Define binning functions for numerical fields
def bin_year(year):
    return "[" + str((year // 5) * 5) + "-" + str(((year // 5) * 5) + 5) + "]"

def bin_price(price):
    return "[" + str(int(price // 5) * 5) + "-" + str((int(price // 5) * 5) + 5) + "]"

def bin_ratings(ratings):
    return "[" + str(int(ratings // 0.5) * 0.5) + "-" + str((int(ratings // 0.5) * 0.5) + 0.5) + "]"

# Apply binning functions to relevant columns
df = df.withColumn("Year", expr("bin_year(releasedDayYear)"))
df = df.withColumn("Price", expr("bin_price(price)"))
df = df.withColumn("Ratings", expr("bin_ratings(ratings)"))

# Define the columns to be considered
important_columns = ["free", "genre", "Price", "minInstalls", "adSupported", "containsAds", "Ratings", "releasedMonth"]

# Generate all combinations of 12 columns
all_combinations = list(itertools.combinations(important_columns, 12))

# Iterate over each combination and generate insights
for subset in all_combinations:
    grouped_df = df.groupBy(*subset).count()
    total_count = df.count()
    filtered_df = grouped_df.filter(col("count") >= 0.02 * total_count)

    # Save the results to a CSV file with formatted output
    output_file_path = "_".join(subset) + "_insights.csv"
    filtered_df.withColumn("Output", expr(
        "concat_ws('; ', " + ", ".join([f"'{col_name}=' || {col_name}" for col_name in subset]) + ", count)"))\
        .select("Output").write.text(output_file_path, mode="overwrite")

# Stop the Spark session
spark.stop()
