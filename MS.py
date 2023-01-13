import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date
import RemoveDuplcates
import warnings
import ImportDF
warnings.simplefilter("ignore")

def processArchive():
    fields = ['Semana','Municipio','ANF','BSC/RNC','Station ID','BTS/NodeB/ENodeB','Banda','Celula','CELL_Fisico','Tecnologia','VOLUME_DADOS_DLUL_ALLOP(Mbyte)','TRAFEGO_VOZ_TIM','ACD_DEN','ACD_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM','THROU_USER_DEN','THROU_USER_NUM','ACV_DEN','ACV_NUM','Peso_ACD','Peso_DROP_VOZ','Peso_DROP_DADOS','Peso_DISP','Peso_ACV']
    fields2 = ['Semana','Municipio','ANF','BSC/RNC','Station ID','BTS/NodeB/ENodeB','Banda','Celula','CELL_Fisico','Tecnologia','VOLUME_DADOS_DLUL_ALLOP(Mbyte)','TRAFEGO_VOZ_TIM','ACD_DEN','ACD_NUM','DROP_VOZ_DEN','DROP_VOZ_NUM','DROP_DADOS_DEN','DROP_DADOS_NUM','DISP_DEN','DISP_NUM','THROU_USER_DEN','THROU_USER_NUM','ACV_DEN','ACV_NUM','Peso_ACD','Peso_DROP_VOZ','Peso_DROP_DADOS','Peso_DISP','Peso_ACV']
    
    pathImport = '/export/MS'
    pathImportSI = os.getcwd() + pathImport
    #print (pathImportSI)
    archiveName = pathImport[8:len(pathImport)]
    #print (archiveName)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    csv_path = os.path.join(script_dir, 'export/MS_ALL/'+archiveName+'.csv')
    #print ('loalding files...\n')
    all_filesSI = glob.glob(pathImportSI + "/*.csv")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    #print (all_filesSI)
    li = []
    df = pd.DataFrame()
    for filename in all_filesSI:
        iter_csv = pd.read_csv(filename, index_col=None,skiprows=0, header=0, encoding="UTF-8", error_bad_lines=False,dtype=str, sep = ';',iterator=True, chunksize=10000, usecols = fields )
        #df = pd.concat([chunk[(chunk[filtrolabel] == filtroValue)] for chunk in iter_csv])
        df = pd.concat([chunk for chunk in iter_csv])
        df2 = df[fields] # ordering labels
        li.append(df2)       
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI.columns = fields2
    frameSI = frameSI.astype(str)

    Cluster = ImportDF.ImportDF(['END ID','NOME_DO_SU'],'/import/Cluster')
    frameSI = pd.merge(frameSI,Cluster, how='left',left_on=['Station ID'],right_on=['END ID'])
    frameSI['Ref1'] = frameSI['Semana'] + frameSI['Celula']

    Ocupacao = ImportDF.ImportDF(['Semana','Celula','CRITICO','ALERTA','BOM'],'/export/MS_OCUPACAO')
    Ocupacao['Ref2'] = Ocupacao['Semana'] + Ocupacao['Celula']
    Ocupacao = Ocupacao.drop(['Semana','Celula'], axis=1)

    frameSI = pd.merge(frameSI,Ocupacao, how='left',left_on=['Ref1'],right_on=['Ref2'])

    frameSI = frameSI.drop(['END ID','Ref1','Ref2'], axis=1)
    
    '''
    KPI_List = ['ACD','DROP_VOZ','DROP_DADOS','DISP','THROU_USER','ACV','ACV']
    frameSI = frameSI.astype(str)
    for KPI in KPI_List:
      frameSI[KPI] = frameSI[KPI+'_NUM'].astype(float) / frameSI[KPI+'_DEN'].astype(float)
    '''





    frameSI.to_csv(csv_path,index=False,header=True,sep=';')

#Baixar arquivo como csv, alterAR CODIGO