from pyspark.sql.types import StringType, DateType
from pyspark.sql import functions as F
from src.app_pyspark import users


def test_transform_users(spark):
    file_path = "data/users.csv"
    df_entrada = spark.read.csv(file_path, header=True, inferSchema=True).limit(100)
    df_saida = users(df_entrada)

    schema = df_saida.schema
    assert schema["user_id"].dataType == StringType()
    assert schema["name"].dataType == StringType()
    assert schema["email"].dataType == StringType()
    assert schema["signup_date"].dataType == DateType()

    # Sem duplicatas
    assert df_saida.count() == df_saida.dropDuplicates().count()

    # Filtros aplicados
    assert df_saida.filter(F.col("user_id").isNull()).count() == 0
    assert df_saida.filter(F.col("email").isNull()).count() == 0

    # Campos normalizados (email em lowercase, name/ city/state trim)
    clean_email = df_saida.select(F.lower(F.trim(F.col("email")))).collect()
    original_email = df_saida.select("email").collect()
    assert clean_email == original_email
