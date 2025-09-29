import pandas as pd

def orders_columns(orders_file_path):
  return list(pd.read_csv(orders_file_path).columns)

if __name__ == "__main__":
  file_path = "/content/drive/MyDrive/projetos/ecommerce-pipeline/include/2025-09-22/orders.csv"
  print("Resultado:", orders_columns(file_path))
