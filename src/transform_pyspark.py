from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType, TimestampType

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