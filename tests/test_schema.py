from src.schema import get_csv_columns

def test_columns_check():
    expected_columns = ['order_id',
    'user_id',
    'product_id',
    'quantity',
    'total_price',
    'order_date',
    ]
    file_path = "../data/orders.csv"
    df_columns = orders_columns(file_path)
    assert df_columns == expected_columns (
        f"Colunas diferentes!\nEsperado: {expected_columns}\nEncontrado: {df_columns}"
    )