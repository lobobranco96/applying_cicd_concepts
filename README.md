# CI e Testes Automatizados

Criei um repositorio que demonstra como aplicar **Continuous Integration (CI)** e **testes automatizados** em um pipeline de dados usando **PySpark**, **Pandas** e **Pytest**.

---

## Estrutura do Projeto

```bash
├── data/
│   └── orders.csv
├── include/
|   ├── actions1.JPG
|   └── ci-ok.JPG
├── src/
│   ├── app_pandas.py
│   └── app_pyspark.py  # Funções de transformação (orders, payments, etc.)
├── tests/
│   ├── conftest.py     # Fixture Spark centralizada
│   ├── test_pyspark_orders.py
│   ├── test_pyspark_payments.py
│   ├── test_pyspark_products.py
│   ├── test_pyspark_users.py
│   ├── test_transform_orders.py
│   └── test_schema.py
└── .github/
    └── workflows/
        └── ci.yml      # Pipeline do GitHub Actions
```

## CI (Continuous Integration)
  - O CI foi implementado usando GitHub Actions.
  - Toda vez que ocorre um push ou pull request para a branch main:
  - O GitHub Actions executa um workflow.
  - Instala dependências do requirements.txt.
  - Configura o PYTHONPATH para acessar o src/.
  - Executa os testes com pytest.
  - Se algum teste falhar, o pipeline é interrompido — garantindo qualidade antes do merge.

## Testes Automatizados
  - PySpark + Pytest: Validamos se os dados transformados estão no formato e regras esperadas.
  - Colunas e Tipos: Verificamos se os tipos das colunas estão corretos após a transformação.
  - Dados limpos: Garantimos que não haja valores nulos em colunas críticas e sem duplicatas.
  - Filtros aplicados: Validamos se regras de negócio (ex.: quantity > 0) foram aplicadas.

Exemplo de teste com pyspark:

```bash
def test_transform_orders(spark):
    file_path = "data/orders.csv"
    df_entrada = spark.read.csv(file_path, header=True, inferSchema=True).limit(100)
    df_saida = orders(df_entrada)

    # Tipos
    assert df_saida.schema["order_id"].dataType == StringType()

    # Sem duplicatas
    assert df_saida.count() == df_saida.dropDuplicates().count()

    # Filtros
    assert df_saida.filter(F.col("quantity") <= 0).count() == 0
```

Como rodar localmente

```bash
# Criar ambiente virtual e instalar dependências
pip install -r requirements.txt
# Rodar testes
pytest
```

## Benefícios
  - Menos bugs em produção.
  - Maior confiabilidade no pipeline de dados.
  - Deploy mais seguro com CI automatizado.
  - Facilidade em revisar pull requests (os testes garantem a qualidade).
  - 
__________________________________________

# CD em breve

__________________________________________
