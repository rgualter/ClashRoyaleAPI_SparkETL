from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, when, expr, to_date
from pyspark.sql.types import *

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Misc Demo") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4j(spark)

    data_list = [("Ravi", "28", "1", "2002"),
                 ("Abdul", "23", "5", "81"),  # 1981
                 ("John", "12", "12", "6"),  # 2006
                 ("Rosy", "7", "8", "63"),  # 1963
                 ("Abdul", "23", "5", "81")]  # 1981

    raw_df = spark.createDataFrame(data_list).toDF("name", "day", "month", "year").repartition(3)
    raw_df.printSchema()

df1 = raw_df.withColumn("id", monotonically_increasing_id())
df1.show()

#método1: 
df2= df1.withColumn("year",expr("""
        case when year <21 then year + 2000
        when year < 100 then year + 1900
        else year
        end"""))
df2.show()

#método2: 
df3= df1.withColumn("year",expr("""
        case when year <21 then cast(year as int)+ 2000
        when year < 100 then cast(year as int) + 1900
        else year
        end"""))
df3.show()


#método3: 
df4= df1.withColumn("year",expr("""
        case when year <21 then year + 2000
        when year < 100 then year  + 1900
        else year
        end""").cast(IntegerType()))
df4.show()
df4.printSchema()

#método4: 
df5= df1.withColumn("day", col("day").cast(IntegerType())) \
        .withColumn("month", col("month").cast(IntegerType())) \
        .withColumn("year", col("year").cast(IntegerType())) 


df6= df5.withColumn("year",expr("""
        case when year <21 then year + 2000
        when year < 100 then year  + 1900
        else year
        end"""))
df6.show()
df6.printSchema()


#método5: 
df7 = df5.withColumn("year", \
        when(col("year") < 21, col("year") + 2000)\
            .when(col("year") <100,  col("year") + 1900)\
                .otherwise(col("year")))
df7.show()


#método6 - adding date of birth column: 

df8 = df7.withColumn("dob", expr("to_date(concat(day,'/',month,'/',year),'d/M/y')"))
df8.show()


#método7 - adding date of birth column / excluding day, month and yeat columns / deduplicate datafram / sort by dob / 
df9 = df7.withColumn("dob", to_date(expr("concat(day,'/',month,'/',year)"),'d/M/y')) \
    .drop("day","month","year") \
    .dropDuplicates(["name","dob"]) \
    .sort(expr("dob desc"))


df9.show()


#Final df:
final_df = raw_df.withColumn("id", monotonically_increasing_id()) \
        .withColumn("day", col("day").cast(IntegerType())) \
        .withColumn("month", col("month").cast(IntegerType())) \
        .withColumn("year", col("year").cast(IntegerType())) \
        .withColumn("year", when(col("year") < 20, col("year") + 2000)
                    .when(col("year") < 100, col("year") + 1900)
                    .otherwise(col("year"))) \
        .withColumn("dob", expr("to_date(concat(day,'/',month,'/',year), 'd/M/y')")) \
        .drop("day", "month", "year") \
        .dropDuplicates(["name", "dob"]) \
        .sort(col("dob").desc())
    # .sort(expr("dob desc")) This doesn't seem to be working

final_df.show()