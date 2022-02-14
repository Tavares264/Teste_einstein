from pyspark.sql import SparkSession, types
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("teste").getOrCreate()

df_exames = spark.read.parquet("./data-lake/curated/hot/tb_exames/")
df_pacientes = spark.read.parquet("./data-lake/curated/hot/tb_pacientes/")


df_exames_por_pacientes = df_pacientes.join(df_exames,df_pacientes.ID_PACIENTE == df_exames.ID_PACIENTE,"full")\
    .select(df_pacientes.ID_PACIENTE,\
    df_pacientes.IC_SEXO,\
    df_pacientes.CD_UF,\
    df_pacientes.CD_PAIS,\
    df_pacientes.CD_MUNICIPIO,\
    df_pacientes.CD_CEPREDUZIDO,\
    df_exames.DE_ORIGEM,\
    df_exames.DE_EXAME,\
    df_exames.DE_RESULTADO,\
    df_exames.CD_UNIDADE,\
    df_exames.DE_VALOR_REFERENCIA,\
    df_exames.DE_ANALITO,\
    df_exames.DT_COLETA)

df_exames_por_pacientes_sp = df_exames_por_pacientes.select("*").filter(df_exames_por_pacientes.CD_UF == "SP")

df_exames_por_pacientes.write.format("parquet") \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .option("encoding", "UTF-8") \
    .partitionBy("CD_PAIS", "CD_UF") \
    .save("./data-lake/service/exames_por_pacientes")

df_exames_por_pacientes_sp.write.format("parquet") \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .option("encoding", "UTF-8") \
    .save("./data-lake/service/exames_por_pacientes_sp")
    