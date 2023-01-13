import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date
import RemoveDuplcates
import warnings
import datetime
warnings.simplefilter("ignore")

def processArchive(Agregacao):
    fields = ['Semana',Agregacao,'ANF','Station ID','BTS/NodeB/ENodeB','Tecnologia','Celula','VOLUME_DADOS_DLUL_ALLOP(Mbyte)','TRAFEGO_VOZ_TIM','ACD_DEN','ACD_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM','THROU_USER_DEN','THROU_USER_NUM','ACV_DEN','ACV_NUM','Peso_ACD','Peso_DROP_VOZ','Peso_DROP_DADOS','Peso_DISP','Peso_ACV','NOME_DO_SU','CRITICO','ALERTA','BOM']
    fields2 = ['Semana',Agregacao,'ANF','Station ID','BTS/NodeB/ENodeB','Tecnologia','Celula','VOLUME_DADOS_DLUL_ALLOP(Mbyte)','TRAFEGO_VOZ_TIM','ACD_DEN','ACD_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM','THROU_USER_DEN','THROU_USER_NUM','ACV_DEN','ACV_NUM','Peso_ACD','Peso_DROP_VOZ','Peso_DROP_DADOS','Peso_DISP','Peso_ACV','NOME_DO_SU','CRITICO','ALERTA','BOM']
    
    pathImport = '/export/MS_ALL'
    pathImportSI = os.getcwd() + pathImport
    #print (pathImportSI)
    archiveName = pathImport[8:len(pathImport)]
    #print (archiveName)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    #print ('loalding files...\n')
    all_filesSI = glob.glob(pathImportSI + "/*.csv")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    print (all_filesSI)
    li = []
    df = pd.DataFrame()
    DateCreation = datetime.datetime.fromtimestamp(os.path.getmtime(all_filesSI[0])).strftime("%Y%m%d")
    for filename in all_filesSI:
        iter_csv = pd.read_csv(filename, index_col=None,skiprows=0, header=0, encoding="UTF-8", error_bad_lines=False,dtype=str, sep = ';',iterator=True, chunksize=10000, usecols = fields )
        #df = pd.concat([chunk[(chunk[filtrolabel] == filtroValue)] for chunk in iter_csv])
        df = pd.concat([chunk for chunk in iter_csv])
        df2 = df[fields] # ordering labels
        li.append(df2)       
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI.columns = fields2
    frameSI = frameSI.astype(str)
    
    
    lista1 = frameSI.columns
    for i in lista1:
      frameSI[i] = [x.replace(',', '.') for x in frameSI[i]]
    
    KPI = ['ACD_DEN','ACD_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM','THROU_USER_DEN','THROU_USER_NUM','ACV_DEN','ACV_NUM']
    for y in KPI:
      frameSI[y[:-4]] = frameSI[y[:-4]+'_NUM'].astype(float)/frameSI[y[:-4]+'_DEN'].astype(float)
      

    
    col_Cluster = frameSI[Agregacao].unique()
    ref = ['Peso_ACD','Peso_DROP_VOZ','Peso_DROP_DADOS','Peso_DISP','Peso_ACV','THROU_USER_NUM']

    Week = frameSI.at[0,'Semana']
    index1 = 0
    frameSI[ref] = frameSI[ref].apply(pd.to_numeric, errors='coerce')
    for agreg in col_Cluster:
      frameSI2 = frameSI.loc[(frameSI[Agregacao] == agreg) & (frameSI['Tecnologia'] == '4G')]

      for ref1 in ref:
        frameSI2 = frameSI2.sort_values([ref1], ascending = [False])
        frameSI2[ref1+'%'] = (frameSI2[ref1] / frameSI2[ref1].sum()) * 100

      #Ofensor less then 5Mbps  
      frameSI2 = frameSI2.loc[frameSI2['THROU_USER'] <=5000]
      frameSI2 = frameSI2.astype(str)  
      
      lista1 = frameSI2.columns
      for i in lista1:
        frameSI2[i] = [x.replace('.', ',') for x in frameSI2[i]]
      
      if index1 == 0:
        frame_ALL = frameSI2.copy()
        index1 = 1
      else:
        frame_ALL = frame_ALL.append(frameSI2,ignore_index = True)
    
    
    csv_path = os.path.join(script_dir, 'export/'+Agregacao+'_W_Ofensor/'+Week+ '_Ofensores.csv')
    frame_ALL.to_csv(csv_path,index=False,header=True,sep=';')

    



