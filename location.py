import boto3
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import SparkSession

def search_addresses(addresses, index_name, language, max_results, bucket_name, file_name):
    # Create a Location client
    location = boto3.client('location')

    # Create a ThreadPoolExecutor
    with ThreadPoolExecutor() as executor:
        # submit a task for each address
        future_to_address = {executor.submit(search_address, address, index_name, language, max_results): address for address in addresses}
        # Create an empty dataframe
        df = pd.DataFrame()
        failed_addresses = []
        for future in concurrent.futures.as_completed(future_to_address):
            address = future_to_address[future]
            try:
                response = future.result()
                # Append the response to the dataframe
                df = df.append(pd.json_normalize(response))
                # Add the original address as a new column
                df['original_address'] = address
            except Exception as exc:
                # Retry the API call up to 5 times
                for i in range(5):
                    try:
                        response = location.search_place_index_for_text(
                            IndexName=index_name,
                            Language=language,
                            MaxResults=max_results,
                            Text=address
                        )
                        df = df.append(pd.json_normalize(response))
                        df['original_address'] = address
                        break
                    except Exception as e:
                        if i == 4:
                            print(f'{address} generated an exception: {e}')
                            failed_addresses.append(address)
    spark = SparkSession.builder.appName("AWS_location_search").getOrCreate()
    df = spark.createDataFrame(df)
    # write the dataframe to parquet
    df.write.mode("append").parquet(f"s3a://{bucket_name}/{file_name}")
    # write the failed addresses to text files in another S3 folder
    failed_addresses_rdd = spark.sparkContext.parallelize(failed_addresses)
    failed_addresses_rdd.coalesce(1).saveAsTextFile(f"s3a://{bucket_name}/failed_addresses/failed_addresses.txt")

def search_address(address, index_name, language, max_results):
    response = location.search_place_index_for_text(
        IndexName=index_name,
        Language=language,
        MaxResults=max_results,
        Text=address
    )
    return response
