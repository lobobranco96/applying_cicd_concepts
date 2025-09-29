from src.app import orders_columns

def test_columns_check():
    expected_columns = ['order_id',
    'user_id',
    'product_id',
    'quantity',
    'total_price',
    'order_date',
    ]
    file_path = "/content/drive/MyDrive/projetos/ecommerce-pipeline/include/2025-09-22/orders.csv"
    df_columns = orders_columns(file_path)
    assert df_columns == expected_columns (
        f"Colunas diferentes!\nEsperado: {expected_columns}\nEncontrado: {df_columns}"
    )