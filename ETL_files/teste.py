import boto3
from io import BytesIO
from pyspark.sql import SparkSession
from lib.logger import Log4j
import pandas as pd
from pyspark.sql.functions import col, concat_ws

class S3DataLoader:
    def __init__(self):
        self.spark = (
            SparkSession.builder.master("local[3]")\
                .appName("SparkSchemaDemo")\
                .getOrCreate()
        )
        self.logger = Log4j(self.spark)
        self.df = None
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")


    def read_s3_parquet_file(self,bucket_name, file_path):
        # sourcery skip: inline-immediately-returned-variable

        s3_uri = f's3a://{bucket_name}/{file_path}'
        df = self.spark.read.parquet(s3_uri)
        return df
    
    #def write_csv_local(self):
    #    df.write \
    #    .format("csv") \
    #    .mode("overwrite") \
    #    .option("path", "output/tetste.csv") \
    #    .save()


    def write_csv_local(self, df):
        
        #df = df.withColumn("array_column_string", concat_ws(",", col("array_column").cast("string")))
        df.select("*").write.format("csv").mode("overwrite").option("header", "true").save("data/output/teste.csv")



bucket_name = 'apiroyale-stage'
file_path = 'APIRoyale/players/sub_type=battlelog/transformed_at=2023-03-05/2023-03-05 19:03:24.614818.parquet'
spark = S3DataLoader()
df = spark.read_s3_parquet_file(bucket_name, file_path)
print(df)
df.show(50)
dfcsv = spark.write_csv_local(df)

#s3://apiroyale-stage/APIRoyale/players/sub_type=battlelog/transformed_at=2023-03-05/2023-03-05 18:35:12.794529.parquet/