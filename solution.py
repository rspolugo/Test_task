from pyspark.sql import SparkSession, dataframe
from pyspark.shell import spark
from pyspark.sql.functions import (
    col,
    avg,
    count,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    FloatType,
    StringType,
)

# 1. Прочитать csv файл: book.csv
path = "books.csv"

schema = StructType(
    [
        StructField("bookID", StringType(), True),
        StructField("title", StringType(), True),
        StructField("authors", StringType(), True),
        StructField("average_rating", FloatType(), True),
        StructField("isbn", StringType(), True),
        StructField("isbn13", IntegerType(), True),
        StructField("language_code", StringType(), True),
        StructField("num_pages", IntegerType(), True),
        StructField("ratings_count", IntegerType(), True),
        StructField("text_reviews_count", IntegerType(), True),
        StructField("publication_date", StringType(), True),
        StructField("publisher", StringType(), True),
    ]
)


class BL_test_task():
    def __init__(self):
        self.spark_session = SparkSession.builder.master("local[*]").appName("beeline").getOrCreate()

    def task_1(self):
        return self.spark_session.read.csv(
            path=path,
            schema=schema,
            header=True,
        )

    def task_2(self, df: dataframe):
        return df.schema

    def task_3(self, df: dataframe):
        return df.count()

    def task_4(self, df: dataframe):
        return df.filter("average_rating > '4.50'").show()

    def task_5(self, df: dataframe):
        return df.select(avg('average_rating')).show()

    def task_6_1(self, df: dataframe):
        return (df.groupBy("title") \
                .agg(count("*").alias("count_of_books")) \
                .where(col("count_of_books") <= 1) \
                .show(truncate=False))

    def task_6_2(self, df: dataframe):
        return (df.groupBy("title") \
                .agg(count("*").alias("count_of_books")) \
                .where(col("count_of_books") >= 1) \
                .where(col("count_of_books") <= 2) \
                .show(truncate=False))


    def task_6_3(self, df: dataframe):
            return (df.groupBy("title") \
                    .agg(count("*").alias("count_of_books")) \
                    .where(col("count_of_books") >= 2) \
                    .where(col("count_of_books") <= 3) \
                    .show(truncate=False))


    def task_6_4(self, df: dataframe):
            return (df.groupBy("title") \
                .agg(count("*").alias("count_of_books")) \
                .where(col("count_of_books") >= 3) \
                .where(col("count_of_books") <= 4) \
                .show(truncate=False))
    def task_6_5(self, df: dataframe):
            return (df.groupBy("title") \
                    .agg(count("*").alias("count_of_books")) \
                    .where(col("count_of_books") >= 4) \
                    .where(col("count_of_books") <= 5) \
                    .show(truncate=False))

test = BL_test_task()
df = test.task_1()
test.task_2(df)
test.task_3(df)
test.task_4(df)
test.task_5(df)
test.task_6_1(df)
test.task_6_2(df)
test.task_6_3(df)
test.task_6_4(df)
test.task_6_5(df)