import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType, TimestampType
from src.transform_pyspark import orders


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()
    yield spark
    spark.stop()


def test_transform_orders(spark):
  file_path = "data/orders.csv"
  df_entrada = spark.read.csv(file_path, header=True, inferSchema=True).limit(100)
  df_saida = orders(df_entrada)

  schema = df_saida.schema
  assert schema["order_id"].dataType == StringType()
  assert schema["quantity"].dataType == IntegerType()
  assert schema["total_price"].dataType == DoubleType()
  assert schema["order_date"].dataType == TimestampType()

  # Sem duplicatas
  assert df_saida.count() == df_saida.dropDuplicates().count()

  # Colunas derivadas
  for c in ["order_year","order_month","order_day"]:
      assert c in df_saida.columns

  # Status limpo
  status_clean = df_saida.select(F.trim(F.lower(F.col("status")))).collect()
  original_status = df_saida.select("status").collect()
  assert status_clean == original_status

  # Sem nulos em colunas críticas
  assert df_saida.filter(F.col("user_id").isNull() | F.col("quantity").isNull() | F.col("total_price").isNull()).count() == 0

  # Filtros aplicados
  assert df_saida.filter(F.col("quantity") <= 0).count() == 0
  assert df_saida.filter(F.col("total_price") <= 0).count() == 0
  assert df_saida.filter(F.col("order_date") > F.current_timestamp()).count() == 0
