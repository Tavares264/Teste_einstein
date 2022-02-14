from zipfile import ZipFile
import os
import shutil
import glob
import pandas as pd

arquivozip = glob.glob('./*.zip')
dt = pd.to_datetime("now").date()

for x in arquivozip:
    try:
        z = ZipFile(x, 'r')
        z.extractall()
        z.close()

        py_files = glob.glob('./*.csv')

        for y in py_files:
            newname = y.replace('.csv','')
            os.rename(y, "./data-lake/stage/" + newname + str(dt) + ".csv")
            os.remove(arquivozip)
    except:
        print("ErroS")