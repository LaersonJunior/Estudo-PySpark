from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Inicializando a sessão do Spark
spark = (
    SparkSession.builder
    .appName("Exemplo de DataFrame")
    .master("local[*]")  # Define o modo de execução local com todos os núcleos disponíveis
    .getOrCreate()
)


