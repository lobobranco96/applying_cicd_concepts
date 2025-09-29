import pandas as pd

def get_csv_columns(file_path: str) -> list[str]:
    """
    Lê um CSV e retorna a lista de colunas.
    """
    df = pd.read_csv(file_path)
    return df.columns.tolist()