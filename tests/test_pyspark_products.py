from pyspark.sql.types import StringType, DoubleType, IntegerType
from src.app_pyspark import products

def test_transform_products(spark):
    file_path = "data/products.csv"
    df_entrada = spark.read.csv(file_path, header=True, inferSchema=True).limit(100)
    df_saida = products(df_entrada)

    schema = df_saida.schema
    assert schema["product_id"].dataType == StringType()
    assert schema["name"].dataType == StringType()
    assert schema["category"].dataType == StringType()
    assert schema["price"].dataType == DoubleType()
    assert schema["stock"].dataType == IntegerType()

    # Sem duplicatas
    assert df_saida.count() == df_saida.dropDuplicates().count()

    # Nome e categoria limpos
    clean_cat = df_saida.select(F.lower(F.trim(F.col("category")))).collect()
    original_cat = df_saida.select("category").collect()
    assert clean_cat == original_cat

    # Filtros aplicados
    assert df_saida.filter(F.col("price") <= 0).count() == 0
    assert df_saida.filter(F.col("stock") < 0).count() == 0
