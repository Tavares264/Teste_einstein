import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import glob
import os


spark=SparkSession.builder.appName("teste").getOrCreate()

arquivo = glob.glob('./data-lake/stage/*.csv')

for x in arquivo:
    if x.lower().find('exames') >= 0:
        tabela = "exames"
    elif x.lower().find('pacientes') >= 0:
        tabela = "pacientes"
    

    df = spark.read.csv(x,sep="|", header=True)

    resul = df.withColumn("DT_CARGA", date_format(current_date(),"yyyy/MM/dd"))

    resul.write.format("parquet") \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .option("encoding", "UTF-8") \
        .save("./data-lake/raw/tb_" + tabela)


    os.rename(x, "./data-lake/stage/historico/" + tabela + str(pd.to_datetime("now").date()).replace("-","") + ".csv")