from pyspark.sql import SparkSession
from pyspark.sql.functions import *


spark=SparkSession.builder.appName("teste").getOrCreate()

    

df = spark.read.parquet("./data-lake/raw/tb_pacientes/")

df_hot = df.select("ID_PACIENTE",\
    "IC_SEXO",\
    "AA_NASCIMENTO",\
    "CD_UF",\
    "CD_PAIS",\
    "DT_CARGA",\
    when(df.CD_MUNICIPIO == "MMMM", "HIDEN").otherwise(df.CD_MUNICIPIO).alias("CD_MUNICIPIO"),\
    when(df.CD_CEPREDUZIDO == "CCCC", "HIDEN").otherwise(df.CD_CEPREDUZIDO).alias("CD_CEPREDUZIDO"))\
    .na.drop("any").where("ID_PACIENTE <> '' and AA_NASCIMENTO <> ''  and CD_PAIS <> '' and CD_MUNICIPIO <> ''")\
    .withColumn("DT_CARGA", to_date(df.DT_CARGA, "yyyy/MM/dd"))

df_rejected = df.select("ID_PACIENTE",\
    "IC_SEXO",\
    "AA_NASCIMENTO",\
    "CD_UF",\
    "CD_PAIS",\
    "DT_CARGA",\
    when(df.CD_MUNICIPIO == "MMMM", "HIDEN").otherwise(df.CD_MUNICIPIO).alias("CD_MUNICIPIO"),\
    when(df.CD_CEPREDUZIDO == "CCCC", "HIDEN").otherwise(df.CD_CEPREDUZIDO).alias("CD_CEPREDUZIDO"))\
    .where("ID_PACIENTE is null or ID_PACIENTE = '' \
    or AA_NASCIMENTO is null or AA_NASCIMENTO = '' \
    or CD_PAIS is null or CD_PAIS = '' \
    or CD_MUNICIPIO is null or CD_MUNICIPIO = ''")\
    .withColumn("DT_CARGA", to_date(df.DT_CARGA, "yyyy/MM/dd"))


df_hot.write.format("parquet") \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .option("encoding", "UTF-8") \
    .save("./data-lake/curated/hot/tb_pacientes")

if df_rejected.count() > 0:
    df_rejected.write.format("parquet") \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .option("encoding", "UTF-8") \
    .save("./data-lake/curated/rejected/tb_pacientes")