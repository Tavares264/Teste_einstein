from unittest import result
from xml.etree.ElementTree import tostring
import matplotlib
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_parquet("./data-lake/service/exames_por_pacientes_sp/")

def isNumber(n):
    try:
        float(n)
    except ValueError:
        return False
    return True

df["FILTRO"] = x = df["DE_RESULTADO"].astype(str).apply(isNumber) 

filtrar = df[df.FILTRO == False]

cont = df["DE_ANALITO"].value_counts()


tabela = pd.pivot_table(data=filtrar, values='FILTRO', columns='DE_ANALITO', aggfunc='count')

print(tabela)
plt.figure(figsize=(8, 6))
plt.hist(cont, bins=range(1, 1000,10))
plt.title('Distribuição de Pesos')
plt.xlabel('Peso')
plt.ylabel('Alunos')
plt.show()