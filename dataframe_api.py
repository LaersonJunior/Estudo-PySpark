from pyspark.sql import SparkSession
from pyspark.sql import functions as Fu
import pandas as pd


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
    df
    .select('Estado - Sigla','Produto','Valor de Venda','Unidade de Medida')
    .withColumn(
        'Valor de Venda',
        Fu.regexp_replace(Fu.col('Valor de Venda'),",",".")
        .cast("float")
    )
    
)

(
    df_precos
    .groupBy(
        Fu.col('Estado - Sigla'),
        Fu.col('Produto'),
        Fu.col('Unidade de Medida')
        
    )
    .agg(
        Fu.min(Fu.col('Valor de Venda')).alias('Menor_valor'),
        Fu.max(Fu.col('Valor de Venda')).alias('Maior_valor')
    )
    .withColumn(
        'diferenca',
        Fu.col('Maior_valor') - Fu.col('Menor_valor')
    )
    
    .orderBy('diferenca',ascending=False)
    
)

df_precos_analises = (
    df_precos
    .groupBy(
        Fu.col('Estado - Sigla'),
        Fu.col('Produto'),
        Fu.col('Unidade de Medida')
        
    )
    .agg(
        Fu.min(Fu.col('Valor de Venda')).alias('Menor_valor'),
        Fu.max(Fu.col('Valor de Venda')).alias('Maior_valor')
    )
    .withColumn(
        'diferenca',
        Fu.col('Maior_valor') - Fu.col('Menor_valor')
    )
    
    .orderBy('diferenca',ascending=False)
    
)

df_pandas = df_precos_analises.toPandas()

df_pandas.to_excel('teste.xlsx',index=False)

print("Finalizado")