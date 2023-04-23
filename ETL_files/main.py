from loader import * 
from transformer import *
from writer import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from _columns_ import *

if __name__ == "__main__":

    spark = SparkSession.builder.master("local[3]").appName("SparkETL").getOrCreate()

    spark._jsc.hadoopConfiguration().set(
        "fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    )
    etl = SparkETL(spark, "apiroyale-raw")
    df = etl.apply_flatten_join_df("2023-04-19") #put the date that needs to back the day (format: "2023-04-19"). Otherwise, None will return today.
    df.show()
    data_writer = S3DataWriter(data = df, format = "parquet", bucket_name = "apiroyale-stage")
    data_writer.write_to_s3()