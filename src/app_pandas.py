import pandas as pd

def get_csv_columns(file_path: str) -> list[str]:
    """
    Lê um CSV e retorna a lista de colunas.
    """
    df = pd.read_csv(file_path)
    return df.columns.tolist()


def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
  """
  Lê um CSV, aplica transformações e retorna um dataframe transformado.
  """

  df = df.astype({
      "order_id": str,
      "user_id": str,
      "product_id": str,
      "quantity": "Int64",
      "total_price": float
  })
  
  # Convertendo order_date para datetime
  df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
  
  # status como string
  df["status"] = df["status"].astype(str)
  
  # Remover duplicatas
  df = df.drop_duplicates()
  
  # Criar colunas a partir de order_date
  df["order_year"] = df["order_date"].dt.year
  df["order_month"] = df["order_date"].dt.month
  df["order_day"] = df["order_date"].dt.day
  
  # Limpar status (trim + lower)
  df["status"] = df["status"].str.strip().str.lower()
  
  # Remover linhas com valores nulos em colunas críticas
  df = df.dropna(subset=["user_id", "quantity", "total_price"])
  
  # Filtrar valores inválidos
  df = df[(df["quantity"] > 0) & (df["total_price"] > 0)]
  
  # Filtrar datas no futuro
  df = df[df["order_date"] <= pd.Timestamp.now()]
  
  return df.reset_index(drop=True)
