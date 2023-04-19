from pyspark.sql.functions import *
from pyspark.sql.window import Window
from _columns_ import *
from loader import *

class SparkETL(SparkFileLoader):
    def __init__(self):
        super().__init__()

    def add_hash_battle_id(self, df):
        self.df = df
        return df.withColumn("hash_id", md5(concat("raw_file_name", "battleTime")))

    def add_file_battle_id(self, df):
        self.df = df
        window_spec = Window.partitionBy("raw_file_name").orderBy("battleTime")
        return df.withColumn("file_battle_id", dense_rank().over(window_spec))

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
        self.df = self.df.select(
            *columns_explode_team, explode(self.df.team).alias(f"{col_explode_1}")
        )

    def explode_team_cards(self):
        self.df = self.df.select(
            *columns_explode_team_cards,
            col("team_princessTowersHitPoints")
            .getItem(0)
            .alias("team_princessTowersHitPoints_1"),
            col("team_princessTowersHitPoints")
            .getItem(1)
            .alias("team_princessTowersHitPoints_2"),
            explode(self.df.team_cards).alias(f"{col_explode_2}"),
        )

    def explode_opponent(self):
        self.df = self.df.select(
            *columns_explode_opponent,
            explode(self.df.opponent).alias(f"{col_explode_3}"),
        )

    def explode_opponent_cards(self):
        self.df = self.df.select(
            *columns_explode_opponent_cards,
            col("opponent_princessTowersHitPoints")
            .getItem(0)
            .alias("opponent_princessTowersHitPoints_1"),
            col("opponent_princessTowersHitPoints")
            .getItem(1)
            .alias("opponent_princessTowersHitPoints_2"),
            explode(self.df.opponent_cards).alias(f"{col_explode_4}"),
        )

    def flatten_join_df(self, date=None):
        self.load_data(date)
        for i in range(6):
            if i in [0, 2, 4]:
                self.df = self.flatten_df(self.df)
            elif i == 1:
                self.explode_team()
            elif i == 3:
                self.explode_team_cards()
            elif i == 5:
                self.df = self.df.select(*columns_team_final)
        team_df = self.df

        self.load_data(date)
        for i in range(6):
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
        team_df = self.add_hash_battle_id(team_df)

        opponent_df = opponent_df.withColumn(
            "opponent_index", monotonically_increasing_id()
        )

        df = team_df.join(
            opponent_df,
            team_df["team_index"] == opponent_df["opponent_index"],
            "inner",
        ).drop("team_index", "opponent_index")

        return df
