import pandas as pd
from src.app_pandas import transform_orders


def test_transform_orders():
    # Lê CSV de teste
    df_entrada = pd.read_csv("data/orders.csv", nrows=100)

    # Executa a transformação
    df_saida = transform_orders(df_entrada)

    # Tipos corretos
    assert df_saida["order_id"].dtype == object
    assert df_saida["quantity"].dtype in ["int64", "Int64"]
    assert df_saida["total_price"].dtype == float
    assert pd.api.types.is_datetime64_any_dtype(df_saida["order_date"])

    # Sem duplicatas
    assert df_saida.duplicated().sum() == 0

    # Colunas derivadas presentes
    for col in ["order_year", "order_month", "order_day"]:
        assert col in df_saida.columns

    # Status limpo (lowercase, sem espaços)
    assert df_saida["status"].str.strip().str.lower().equals(df_saida["status"])

    # Sem valores nulos em colunas críticas
    assert df_saida[["user_id", "quantity", "total_price"]].isnull().sum().sum() == 0

    # Filtros aplicados
    assert (df_saida["quantity"] > 0).all()
    assert (df_saida["total_price"] > 0).all()
    assert (df_saida["order_date"] <= pd.Timestamp.now()).all()
