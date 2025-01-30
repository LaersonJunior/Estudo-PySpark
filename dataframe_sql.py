from pyspark.sql import SparkSession
import pandas as pd

# Inicializando a sessão do Spark
spark = (
    SparkSession.builder
    .master("local[*]")  # Define o modo de execução local com todos os núcleos disponíveis
    .appName("spark_dataframe_sql")
    .getOrCreate()
)

# Lendo o arquivo CSV
df = (
    spark
    .read
    .option('delimiter', ';')
    .option('header', 'true')
    .option('inferSchema', 'true')  # Correção: inferSchema
    .option('encoding', 'ISO-8859-1')  # Correção: encoding
    .csv('./Dados/precos-gasolina-etanol-12.csv')
)

# Criando uma view temporária
df.createOrReplaceTempView('combustiveis')

# Realizando a primeira consulta SQL
df_transformado = spark.sql("""
    SELECT
        `Estado - Sigla`,
        `Produto`,
        regexp_replace(`Valor de Venda`, ",", ".") as `Valor de Venda`,
        `Unidade de Medida`
    FROM
        combustiveis
""")

# Criando uma nova view temporária com os dados transformados
df_transformado.createOrReplaceTempView('view_precos')

# Exibindo os primeiros 5 registros da view_precos
view_precos = spark.sql("""
    SELECT
        `Estado - Sigla`,
        `Produto`,
        `Valor de Venda`,
        `Unidade de Medida`
    FROM
        view_precos
""").show(5)

# Realizando a consulta para calcular a diferença entre os preços
df_precos_diferenca = spark.sql("""
    SELECT
        `Estado - Sigla`,
        `Produto`,
        `Unidade de Medida`,
        MIN(`Valor de Venda`) as Menor_Valor,
        MAX(`Valor de Venda`) as Maior_Valor,
        MAX(`Valor de Venda`) - MIN(`Valor de Venda`) as Diferenca
    FROM
        view_precos
    GROUP BY
        `Estado - Sigla`, `Produto`, `Unidade de Medida`
    ORDER BY
        Diferenca DESC
""").show(5)