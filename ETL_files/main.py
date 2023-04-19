from loader import * 
from transformer import *
from writer import *



if __name__ == "__main__":
    etl = SparkETL()
    df = etl.flatten_join_df()
    data_writer = S3DataWriter(df, "parquet")
    data_writer.write_parquet_to_s3()
