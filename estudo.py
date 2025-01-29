from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession

# Inicializando a sessão do Spark
spark = (
    SparkSession.builder
    .appName("Exemplo de DataFrame")
    .master("local[*]")  # Define o modo de execução local com todos os núcleos disponíveis
    .getOrCreate()
)

# Dados a serem inseridos no DataFrame
data = [
    ("James", "", "Smith", "36636", "M", 3000),
    ("Michael", "Rose", "", "40288", "M", 4000),
    ("Robert", "", "Williams", "42114", "M", 4000),
    ("Maria", "Anne", "Jones", "39192", "F", 4000),
    ("Jen", "Mary", "Brown", "", "F", -1)
]

# Definindo o schema do DataFrame
schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("middlename", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", IntegerType(), True)
])

# Criando o DataFrame
df = spark.createDataFrame(data=data, schema=schema)
