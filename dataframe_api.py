from pyspark.sql import SparkSession
from pyspark.sql import functions as Fu


# Inicializando a sessão do Spark
spark = (
    SparkSession.builder
    .master("loca[*]")
    .appName("spark_dataframe_api")
    .master("local[*]")  # Define o modo de execução local com todos os núcleos disponíveis
    .getOrCreate()
)

df=(
    spark
    .read
    .option('delimiter',';')
    .option('header','true')
    .option('inferSchama','true')
    .option('econding','ISO-8859-1')
    .csv('./Dados/precos-gasolina-etanol-12.csv')
)

df_precos = (
    df.select('Estado - Sigla','Produto','Valor de Compra','Valor de Venda','Unidade de Medida')
)

