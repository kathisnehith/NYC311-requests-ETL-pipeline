from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
import time
import requests
import argparse
from datetime import datetime, timedelta

# Function to calculate start and end timestamps for a given month/year
def get_month_timestamps(year, month):
    start_date = datetime(year, month, 1)
    end_date = (start_date + timedelta(days=31)).replace(day=1) - timedelta(days=1)
    return start_date.strftime("%Y-%m-%dT00:00:00.000"), end_date.strftime("%Y-%m-%dT23:59:59.999")

# Function to fetch data
def fetch_data(api_url, app_token, start_timestamp, end_timestamp, offset, limit):
    params = {
        "$limit": limit,
        "$offset": offset,
        "$where": f"created_date >= '{start_timestamp}' AND created_date <= '{end_timestamp}'"
    }
    headers = {"X-App-Token": app_token}
    time.sleep(2)
    response = requests.get(api_url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error fetching data: {response.text}")

# Function to process data
def process_data(spark, api_url, app_token, start_timestamp, end_timestamp, offset, limit):
    data = fetch_data(api_url, app_token, start_timestamp, end_timestamp, offset, limit)
    print(f"Fetched {len(data)} records.")
    if data:
        return spark.createDataFrame(data)

def main(year, batch_size, total_records):
    service_account_key_path = "gs://auth_service_key_seriousprojectid/serious-unison-441416-j6-556d0d88d23e.json"
    api_url = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
    app_token = "18jVxe53wx2bmmv3JUnqbc3XL"
    gcs_bucket = "gs://nyc-311-requests/raw"
    
    # Initialize Spark session with GCS configuration
    spark = SparkSession.builder \
        .appName("NYC fetch Updatedrecords") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", service_account_key_path) \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.23.2") \
        .config("spark.sql.debug.maxToStringFields", "2000") \
        .config("spark.executor.memory", "5g") \
        .config("spark.executor.cores", "2") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    for month in range(1, 13):  # Iterate through all months of the year
        start_timestamp, end_timestamp = get_month_timestamps(year, month)
        print(f"Fetching data for {year}-{month:02d}: {start_timestamp} to {end_timestamp}")

        temp_dfs = []
        for offset in range(0, total_records, batch_size):
            temp_df = process_data(spark, api_url, app_token, start_timestamp, end_timestamp, offset, batch_size)
            if temp_df:
                temp_dfs.append(temp_df)
            else:
                break  # Exit if no more data is available
            time.sleep(2)

        # Combine all processed batches into a single DataFrame
        if temp_dfs:
            combined_df = temp_dfs[0].drop('location')
            for df in temp_dfs[1:]:
                combined_df = combined_df.union(df.drop('location'))

            # Write data to a Parquet file for the month
            output_path = f"{gcs_bucket}/nyc311-datafetch-{year}.parquet"
            combined_df.coalesce(1).write.parquet(output_path, mode="append")
            print(f"Data for {year}-{month:02d} saved to {output_path}")
        else:
            print(f"No data found for {year}-{month:02d}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and process NYC 311 data.")
    parser.add_argument("--year", type=int, required=True, help="Year for which to fetch data.")
    parser.add_argument("--batch_size", type=int, default=350000, help="Maximum number of records to fetch in a single request.")
    parser.add_argument("--total_records", type=int, default=600000, help="Maximum number of records to fetch for a month.")

    args = parser.parse_args()
    main(args.year, args.batch_size, args.total_records)

# arugments to pass: (each line is a separate command)
'''
--year
2025
--batch_size
350000
--total_records
600000
'''