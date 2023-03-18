from io import BytesIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from lib.logger import Log4j
import boto3, datetime, logging
from _columns_ import *

s3 = boto3.client("s3")
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class SparkSchemaDemo:
    def __init__(self):
        self.spark = (
            SparkSession.builder.master("local[3]")\
                .appName("SparkSchemaDemo")\
                .getOrCreate()
        )
        self.logger = Log4j(self.spark)
        self.df = None
        self.spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

    def add_raw_file_name(self, df):
        return df.withColumn("raw_file_name", input_file_name())

    def load_data(self, path):
        self.df = self.spark.read.json(path)
        self.df = self.add_raw_file_name(self.df)
        #self.df = self.df.withColumn("raw_file_name", input_file_name())

    def flatten_df(self, nested_df):
        stack = [((), nested_df)]
        columns = []

        while stack:
            parents, df = stack.pop()

            flat_cols = [
                col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],)))
                for c in df.dtypes
                if c[1][:6] != "struct"
            ]

            nested_cols = [c[0] for c in df.dtypes if c[1][:6] == "struct"]

            columns.extend(flat_cols)

            for nested_col in nested_cols:
                projected_df = df.select(f"{nested_col}.*")
                stack.append((parents + (nested_col,), projected_df))

        return nested_df.select(columns)

    def explode_team(self):
        self.df = self.df.select(*columns_explode_team, explode(self.df.team).alias(f"{col_explode_1}"))

    def explode_team_cards(self):
        self.df = self.df.select(
            *columns_explode_team_cards,
            col("team_princessTowersHitPoints").getItem(0).alias("team_princessTowersHitPoints_1"),
            col("team_princessTowersHitPoints").getItem(1).alias("team_princessTowersHitPoints_2"),
            explode(self.df.team_cards).alias(f"{col_explode_2}"),
        )

    def explode_opponent(self):
        self.df = self.df.select(*columns_explode_opponent, explode(self.df.opponent).alias(f"{col_explode_3}"))

    def explode_opponent_cards(self):
        self.df = self.df.select(
            *columns_explode_opponent_cards,
            col("opponent_princessTowersHitPoints").getItem(0).alias("opponent_princessTowersHitPoints_1"),
            col("opponent_princessTowersHitPoints").getItem(1).alias("opponent_princessTowersHitPoints_2"),
            explode(self.df.opponent_cards).alias(f"{col_explode_4}"),
        )

    def add_file_battle_id(self):
        window_spec = Window.partitionBy("raw_file_name").orderBy("battleTime")
        self.df = self.df.withColumn("file_battle_id", dense_rank().over(window_spec))

    def run(self, path):
        self.load_data(path)
        for i in range(6):
            print(i)
            if i in [0, 2, 4, 6, 8]:
                self.df = self.flatten_df(self.df)
            elif i == 1:
                self.explode_team()
            elif i == 3:
                self.explode_team_cards()
            elif i == 5:
                self.df = self.df.select(*columns_team_final)
        return self.df
    
    def run_opponent(self, path):
        self.load_data(path)
        for i in range(6):
            print(i)
            if i in [0, 2, 4, 6, 8]:
                self.df = self.flatten_df(self.df)
            elif i == 1:
                self.explode_opponent()
            elif i == 3:
                self.explode_opponent_cards()
            elif i == 5:
                self.df = self.df.select(*columns_opponent_final)
        return self.df

    def run_union(self, path):
        self.load_data(path)
        for i in range(6):
            print(i)
            if i in [0, 2, 4]:
                self.df = self.flatten_df(self.df)
            elif i == 1:
                self.explode_team()
            elif i == 3:
                self.explode_team_cards()
            elif i == 5:
                self.df = self.df.select(*columns_team_final)
        team_df = self.df

        self.load_data(path)
        for i in range(6):
            print(i)
            if i in [0, 2, 4]:
                self.df = self.flatten_df(self.df)
            elif i == 1:
                self.explode_opponent()
            elif i == 3:
                self.explode_opponent_cards()
            elif i == 5:
                self.df = self.df.select(*columns_opponent_final_only_diferente_columns)
        opponent_df = self.df

        team_df = team_df.withColumn("team_index", monotonically_increasing_id())
        opponent_df = opponent_df.withColumn("opponent_index", monotonically_increasing_id())

        return team_df.join(
            opponent_df,
            team_df["team_index"] == opponent_df["opponent_index"],
            "inner",
        ).drop("team_index", "opponent_index")

    

class S3DataWriter():
    def __init__(self, data, format):
        super().__init__()
        self.buffer = BytesIO()
        self.now = datetime.datetime.now()
        self.date_str = self.now.date()  
        self.client = boto3.client("s3")
        self.bucket_name = "apiroyale-stage"
        self.format = f"{format}"
        self.key = f"APIRoyale/players/sub_type=battlelog/transformed_at={self.date_str}/{self.now}.{self.format}"
        self.data = data

    def write_parquet_to_s3(self):
        s3_path = self.key
        buffer = self.buffer
        df = self.data
        df.write.mode("overwrite").parquet(f"s3a://{self.bucket_name}/{s3_path}")
        self.client.upload_fileobj(buffer, self.bucket_name, self.key)
        logger.info(f"Writing parquet data to Bucket:{self.bucket_name}/{s3_path}")

    def write_csv_local(self):
        df = self.data
        df.select("*").write.format("csv").mode("overwrite").option("header", "true").save("data/output/teste.csv")


if __name__ == "__main__":
    path = "ETL_files/data/*.json"
    spark_schema_demo = SparkSchemaDemo()
    df = spark_schema_demo.run_union(path)
    df.show()
    data_writer = S3DataWriter(df, "parquet")
    data_writer.write_parquet_to_s3()
    data_writer.write_csv_local()