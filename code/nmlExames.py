from pyspark.sql import SparkSession, types
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("teste").getOrCreate()

def nmlCampo(x):
    y = " ".join(x.split()).lower()
    return(y)

nmlCampoUDF = udf(lambda x: nmlCampo(x)) 

df = spark.read.parquet("./data-lake/raw/tb_exames/")


df_hot = df.select("ID_PACIENTE",\
    "DE_ORIGEM",\
    "DE_EXAME",\
    "DE_RESULTADO",\
    "CD_UNIDADE",\
    "DE_VALOR_REFERENCIA",\
    nmlCampoUDF(df.DE_ANALITO).alias("DE_ANALITO"),\
    "DT_COLETA",\
    "DT_CARGA").na.drop("any").where("DT_COLETA <> '' and DE_ORIGEM <> '' and DE_EXAME <> '' and DE_RESULTADO <> ''")\
    .withColumn("DT_COLETA", to_date(df.DT_COLETA, "dd/MM/yyyy"))\
    .withColumn("DT_CARGA", to_date(df.DT_CARGA, "yyyy/MM/dd"))


df_rejected = df.select("*").where("DT_COLETA is null or DT_COLETA = '' \
    or DE_ORIGEM is null or DE_ORIGEM = '' \
    or DE_EXAME is null or DE_EXAME = '' \
    or DE_RESULTADO is null or DE_RESULTADO = ''")\
    .withColumn("DT_COLETA", to_date(df.DT_COLETA, "dd/MM/yyyy"))\
    .withColumn("DT_CARGA", to_date(df.DT_CARGA, "yyyy/MM/dd"))


df_hot.write.format("parquet") \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .option("encoding", "UTF-8") \
    .save("./data-lake/curated/hot/tb_exames")

if df_rejected.count() > 0:
    df_rejected.write.format("parquet") \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .option("encoding", "UTF-8") \
    .save("./data-lake/curated/rejected/tb_exames")
    