from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, DoubleType, TimestampType
from src.app_pyspark import payments  

def test_transform_payments(spark):
    file_path = "data/payments.csv"
    df_entrada = spark.read.csv(file_path, header=True, inferSchema=True).limit(100)
    df_saida = payments(df_entrada)

    schema = df_saida.schema
    assert schema["payment_id"].dataType == StringType()
    assert schema["order_id"].dataType == StringType()
    assert schema["payment_method"].dataType == StringType()
    assert schema["amount"].dataType == DoubleType()
    assert schema["paid_at"].dataType == TimestampType()

    # Sem duplicatas
    assert df_saida.count() == df_saida.dropDuplicates().count()

    # Colunas derivadas
    for c in ["paid_year", "paid_month", "paid_day"]:
        assert c in df_saida.columns

    # Métodos de pagamento limpos (lowercase)
    clean = df_saida.select(F.lower(F.col("payment_method"))).collect()
    original = df_saida.select("payment_method").collect()
    assert clean == original

    # Filtros aplicados
    assert df_saida.filter(F.col("amount") <= 0).count() == 0
    assert df_saida.filter(F.col("paid_at") > F.current_timestamp()).count() == 0
