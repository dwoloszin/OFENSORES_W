import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date
import RemoveDuplcates
import warnings
warnings.simplefilter("ignore")
import removeBlanks

def processArchive():
    fields = ['Semana do Ano','Município','ANF','BSC/RNC','Station ID','RAN Node','Banda','Célula','Tecnologia','VOLUME_DADOS_DLUL_ALLOP 4G - Mbyte','TRAFEGO_VOZ_TIM 4G','ACD_LT_DEN1','ACD_LT_DEN2','ACD_LT_NUM1','ACD_LT_NUM2','ACV_LT_DEN','ACV_LT_NUM','THROU_USER_PDCP_DL_DEN','THROU_USER_PDCP_DL_NUM','DROP_ERAB_VOLTE_NUM','DROP_ERAB_VOLTE_DEN','DROP_ERAB_DEN','DROP_ERAB_NUM','DISP_COUNTER_TOTAL_DEN 4G (sem filtro OPER)','DISP_COUNTER_TOTAL_NUM 4G (sem filtro OPER)']
    fields2 = ['Semana','Municipio','ANF','BSC/RNC','Station ID','BTS/NodeB/ENodeB','Banda','Celula','Tecnologia','VOLUME_DADOS_DLUL_ALLOP(Mbyte)','TRAFEGO_VOZ_TIM','ACD_LT_DEN1','ACD_LT_DEN2','ACD_LT_NUM1','ACD_LT_NUM2','ACV_DEN','ACV_NUM','THROU_USER_DEN','THROU_USER_NUM','DROP_VOZ_NUM','DROP_VOZ_DEN','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM']
    
    pathImport = '/import/4G'
    pathImportSI = os.getcwd() + pathImport
    #print (pathImportSI)
    archiveName = pathImport[8:len(pathImport)]
    #print (archiveName)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    csv_path = os.path.join(script_dir, 'export/MS/'+archiveName+'.csv')
    #print ('loalding files...\n')
    all_filesSI = glob.glob(pathImportSI + "/*.csv")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    #print (all_filesSI)
    li = []
    df = pd.DataFrame()
    for filename in all_filesSI:
        iter_csv = pd.read_csv(filename, index_col=None,skiprows=2, header=0, encoding="UTF-8",thousands='.', error_bad_lines=False,dtype=str, sep = ';',iterator=True, chunksize=10000, usecols = fields )
        #df = pd.concat([chunk[(chunk[filtrolabel] == filtroValue)] for chunk in iter_csv])
        
        df = pd.concat([chunk for chunk in iter_csv])
        df = df.fillna(0)
        df2 = df[fields] # ordering labels
        li.append(df2)       
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI.columns = fields2 
    frameSI = frameSI.astype(str)
    frameSI['CellSector'] = frameSI['Celula'].str[-1:]
    frameSI['Semana'] = frameSI['Semana'].str[-4:] + frameSI['Semana'].str[:3]
    frameSI['CELL_Fisico'] = frameSI['CellSector'].map({'0':'0', '1':'1', '2':'2', '3':'3', 'A':'1', 'B':'2', 'C':'3', 'D':'4', 'E':'1', 'F':'2', 'G':'3', 'H':'4', 'I':'1', 'J':'2', 'K':'3', 'L':'4', 'M':'1', 'N':'2', 'P':'3', 'Q':'1', 'R':'2', 'S':'3', 'T':'4', 'U':'2', 'V':'3', 'W':'0', 'X':'1', 'Y':'2', 'Z':'3'})
    

    frameSI['TRAFEGO_VOZ_TIM'] = frameSI['TRAFEGO_VOZ_TIM'].apply(lambda x: x.replace(".","").replace(",","."))
    frameSI['TRAFEGO_VOZ_TIM'] = frameSI['TRAFEGO_VOZ_TIM'].astype(float)


    frameSI['VOLUME_DADOS_DLUL_ALLOP(Mbyte)'] = frameSI['VOLUME_DADOS_DLUL_ALLOP(Mbyte)'].apply(lambda x: x.replace(".","").replace(",","."))
    frameSI['VOLUME_DADOS_DLUL_ALLOP(Mbyte)'] = frameSI['VOLUME_DADOS_DLUL_ALLOP(Mbyte)'].astype(float)
    frameSI['VOLUME_DADOS_DLUL_ALLOP(Mbyte)'] = frameSI['VOLUME_DADOS_DLUL_ALLOP(Mbyte)'].astype(int)



    lista1 = ['ACD_LT_DEN1','ACD_LT_DEN2','ACD_LT_NUM1','ACD_LT_NUM2','ACV_DEN','ACV_NUM','THROU_USER_DEN','THROU_USER_NUM','DROP_VOZ_NUM','DROP_VOZ_DEN','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM']
    for i in lista1:
      frameSI[i] = [x.replace('.', '') for x in frameSI[i]]
      frameSI[i] = [x.replace('(', '') for x in frameSI[i]]
      frameSI[i] = [x.replace(')', '') for x in frameSI[i]]


    frameSI['ACD_DEN'] = frameSI['ACD_LT_DEN1'].astype(np.int64) * frameSI['ACD_LT_DEN2'].astype(np.int64) 
    frameSI['ACD_NUM'] = frameSI['ACD_LT_NUM1'].astype(np.int64) * frameSI['ACD_LT_NUM2'].astype(np.int64) 

    frameSI['Peso_ACD'] = (frameSI['ACD_LT_DEN1'].astype(np.int64) + frameSI['ACD_LT_DEN2'].astype(np.int64)) - (frameSI['ACD_LT_NUM1'].astype(np.int64) + frameSI['ACD_LT_NUM2'].astype(np.int64))
    frameSI['Peso_DROP_VOZ'] = frameSI['DROP_VOZ_DEN'].astype(np.int64) - frameSI['DROP_VOZ_NUM'].astype(np.int64)
    frameSI['Peso_DROP_DADOS'] = frameSI['DROP_DADOS_DEN'].astype(np.int64) - frameSI['DROP_DADOS_NUM'].astype(np.int64)
    frameSI['Peso_DISP'] = frameSI['DISP_DEN'].astype(np.int64) - frameSI['DISP_NUM'].astype(np.int64)
    frameSI['Peso_ACV'] = frameSI['ACV_DEN'].astype(np.int64) - frameSI['ACV_NUM'].astype(np.int64)
    

    frameSI = frameSI.drop(['CellSector','ACD_LT_DEN1','ACD_LT_DEN2','ACD_LT_NUM1','ACD_LT_NUM2'], axis=1)
    

    frameSI.to_csv(csv_path,index=False,header=True,sep=';')

#Baixar arquivo como csv, alterAR CODIGO