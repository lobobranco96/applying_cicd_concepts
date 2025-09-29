from src.schema import get_csv_columns

def test_expected_columns():
    file_path = "data/orders.csv"
    
    expected_columns = ['order_id',
    'user_id',
    'product_id',
    'quantity',
    'total_price',
    'order_date',
    ]

    df_columns = get_csv_columns(file_path)
    assert df_columns == expected_columns, \
        f"Colunas diferentes!\nEsperado: {expected_columns}\nEncontrado: {df_columns}"
