from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    DateType,
)


def orders(df):

    df_casted = (
        df.withColumn("order_id", F.col("order_id").cast(StringType()))
        .withColumn("user_id", F.col("user_id").cast(StringType()))
        .withColumn("product_id", F.col("product_id").cast(StringType()))
        .withColumn("quantity", F.col("quantity").cast(IntegerType()))
        .withColumn("total_price", F.col("total_price").cast(DoubleType()))
        .withColumn("order_date", F.col("order_date").cast(TimestampType()))
        .withColumn("status", F.col("status").cast(StringType()))
    )
    df_transformed = (
        df_casted.dropDuplicates()
        .withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
        .withColumn("order_day", F.dayofmonth("order_date"))
        .withColumn("status", F.trim(F.lower("status")))
        .na.drop(subset=["user_id", "quantity", "total_price"])
        .filter(F.col("quantity") > 0)
        .filter(F.col("total_price") > 0)
        .filter(F.col("order_date") <= F.current_timestamp())
    )

    return df_transformed


def payments(df):
    df_casted = (
        df.withColumn("payment_id", F.col("payment_id").cast(StringType()))
        .withColumn("order_id", F.col("order_id").cast(StringType()))
        .withColumn("payment_method", F.col("payment_method").cast(StringType()))
        .withColumn("amount", F.col("amount").cast(DoubleType()))
        .withColumn("paid_at", F.col("paid_at").cast(TimestampType()))
    )

    df_transformed = (
        df_casted.dropDuplicates()
        .withColumn("paid_year", F.year("paid_at"))
        .withColumn("paid_month", F.month("paid_at"))
        .withColumn("paid_day", F.dayofmonth("paid_at"))
        .withColumn("payment_method", F.lower(F.col("payment_method")))
        .filter(F.col("amount").isNotNull() & (F.col("amount") > 0))
        .filter(
            F.col("paid_at").isNotNull() & (F.col("paid_at") <= F.current_timestamp())
        )
    )

    return df_transformed


def products(df):
    df_casted = (
        df.withColumn("product_id", F.col("product_id").cast(StringType()))
        .withColumn("name", F.col("name").cast(StringType()))
        .withColumn("category", F.col("category").cast(StringType()))
        .withColumn("price", F.col("price").cast(DoubleType()))
        .withColumn("stock", F.col("stock").cast(IntegerType()))
    )

    df_transformed = (
        df_casted.dropDuplicates()
        .withColumn("category", F.lower(F.trim(F.col("category"))))
        .withColumn("name", F.trim(F.col("name")))
        .filter(F.col("price").isNotNull() & (F.col("price") > 0))
        .filter(F.col("stock").isNotNull() & (F.col("stock") >= 0))
    )
    return df_transformed


def users(df):
    df_casted = (
        df.withColumn("user_id", F.col("user_id").cast(StringType()))
        .withColumn("name", F.col("name").cast(StringType()))
        .withColumn("email", F.col("email").cast(StringType()))
        .withColumn("signup_date", F.col("signup_date").cast(DateType()))
        .withColumn("city", F.col("city").cast(StringType()))
        .withColumn("state", F.col("state").cast(StringType()))
    )

    df_transformed = (
        df_casted.dropDuplicates()
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("email").isNotNull())
        .withColumn("name", F.trim(F.col("name")))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("city", F.trim(F.col("city")))
        .withColumn("state", F.trim(F.col("state")))
    )
    return df_transformed
