import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark session with optimized configurations
spark = SparkSession.builder \
    .appName("NYC311DataFetch") \
    .config("spark.executor.memory", "5g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.initialExecutors", "2") \
    .config("spark.dynamicAllocation.minExecutors", "2") \
    .config("spark.dynamicAllocation.maxExecutors", "10") \
    .config("spark.hadoop.fs.gs.block.size", "67108864") \
    .getOrCreate()

# API URL and App Token
api_url = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
app_token = "api_token"

# Function to fetch data
def fetch_data(offset, limit):
    params = {
        "$limit": limit,
        "$offset": offset,
        "$$app_token": app_token
    }
    headers = {
        "X-App-Token": app_token
    }
    response = requests.get(api_url, headers=headers, params=params)
    print(f"Fetched batch {offset}")
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error fetching data: {response.text}")

# Function to process data
def process_data(offset, limit):
    data = fetch_data(offset, limit)
    if data:
        rdd = spark.sparkContext.parallelize(data)
        df = spark.read.json(rdd)
        return df

# Fetch and process data in batches
batch_size = 100000
total_records = 1000000
temp_dfs = []

for offset in range(0, total_records, batch_size):
    temp_df = process_data(offset, batch_size)
    temp_dfs.append(temp_df)

# Combine all processed batches into a single DataFrame
combined_df = temp_dfs[0]
for df in temp_dfs[1:]:
    combined_df = combined_df.union(df)
    print(f"Combined {df.count()} records")

# Optionally, repartition the combined DataFrame for better parallelism
combined_df = combined_df.repartition(200)
print(f"Repartitioned to {combined_df.rdd.getNumPartitions()} partitions")

# Write the combined DataFrame to a single Parquet file
output_path = "gs://nyc-311-requests/raw/combined_data_1M.parquet"
combined_df.coalesce(1).write.parquet(output_path, mode="overwrite")
