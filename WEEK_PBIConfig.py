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

def processArchive():
    fields = ['Semana','NOME_DO_SU','ANF','Tecnologia','2G_ACD_NOME_DO_SU','2G_DROP_VOZ_NOME_DO_SU','2G_DROP_DADOS_NOME_DO_SU','2G_DISP_NOME_DO_SU','2G_THROU_USER_NOME_DO_SU','2G_ACV_NOME_DO_SU','2G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_NOME_DO_SU','2G_TRAFEGO_VOZ_TIM_NOME_DO_SU','2G_ACD_(m)NOME_DO_SU','2G_DROP_VOZ_(m)NOME_DO_SU','2G_DROP_DADOS_(m)NOME_DO_SU','2G_DISP_(m)NOME_DO_SU','2G_THROU_USER_(m)NOME_DO_SU','2G_ACV_(m)NOME_DO_SU','2G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_(m)NOME_DO_SU','2G_TRAFEGO_VOZ_TIM_(m)NOME_DO_SU','3G_ACD_NOME_DO_SU','3G_DROP_VOZ_NOME_DO_SU','3G_DROP_DADOS_NOME_DO_SU','3G_DISP_NOME_DO_SU','3G_THROU_USER_NOME_DO_SU','3G_ACV_NOME_DO_SU','3G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_NOME_DO_SU','3G_TRAFEGO_VOZ_TIM_NOME_DO_SU','3G_ACD_(m)NOME_DO_SU','3G_DROP_VOZ_(m)NOME_DO_SU','3G_DROP_DADOS_(m)NOME_DO_SU','3G_DISP_(m)NOME_DO_SU','3G_THROU_USER_(m)NOME_DO_SU','3G_ACV_(m)NOME_DO_SU','3G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_(m)NOME_DO_SU','3G_TRAFEGO_VOZ_TIM_(m)NOME_DO_SU','4G_ACD_NOME_DO_SU','4G_DROP_VOZ_NOME_DO_SU','4G_DROP_DADOS_NOME_DO_SU','4G_DISP_NOME_DO_SU','4G_THROU_USER_NOME_DO_SU','4G_ACV_NOME_DO_SU','4G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_NOME_DO_SU','4G_TRAFEGO_VOZ_TIM_NOME_DO_SU','4G_ACD_(m)NOME_DO_SU','4G_DROP_VOZ_(m)NOME_DO_SU','4G_DROP_DADOS_(m)NOME_DO_SU','4G_DISP_(m)NOME_DO_SU','4G_THROU_USER_(m)NOME_DO_SU','4G_ACV_(m)NOME_DO_SU','4G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_(m)NOME_DO_SU','4G_TRAFEGO_VOZ_TIM_(m)NOME_DO_SU','4G_CRITICO_NOME_DO_SU','4G_ALERTA_NOME_DO_SU','4G_BOM_NOME_DO_SU','4G_OCUPACAO_NOME_DO_SU','OffLoad4G','OffLoad4G_(m)','OffLoad4G_Voz','OffLoad4G_Voz(m)']
    pathImport = '/export/Process_All_W'
    pathImportSI = os.getcwd() + pathImport
    #print (pathImportSI)
    archiveName = pathImport[8:len(pathImport)]
    #print (archiveName)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    csv_path = os.path.join(script_dir, 'export/Process_All_W_PBI/'+archiveName+'.xlsx')

    df = pd.DataFrame()
    filename = 'C:\\Users\\f8059678\\OneDrive - TIM\\Dario\\@_PYTHON\\OFENSORES_W/export/Process_All_W\\Consolidado_ClusterSP_Week.xlsx'
    data = pd.read_excel(filename,sheet_name = 'import',usecols = fields)
    frameSI = df.append(data,ignore_index=True)  
    frameSI = frameSI.astype(str)

    

    #frameSI[["a", "b"]] = frameSI[["a", "b"]].apply(pd.to_numeric)
    cleanInt = ['2G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_NOME_DO_SU','2G_TRAFEGO_VOZ_TIM_NOME_DO_SU','2G_THROU_USER_(m)NOME_DO_SU','2G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_(m)NOME_DO_SU','2G_TRAFEGO_VOZ_TIM_(m)NOME_DO_SU','3G_THROU_USER_NOME_DO_SU','3G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_NOME_DO_SU','3G_TRAFEGO_VOZ_TIM_NOME_DO_SU','3G_THROU_USER_(m)NOME_DO_SU','3G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_(m)NOME_DO_SU','3G_TRAFEGO_VOZ_TIM_(m)NOME_DO_SU','4G_THROU_USER_NOME_DO_SU','4G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_NOME_DO_SU','4G_TRAFEGO_VOZ_TIM_NOME_DO_SU','4G_THROU_USER_(m)NOME_DO_SU','4G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_(m)NOME_DO_SU','4G_TRAFEGO_VOZ_TIM_(m)NOME_DO_SU','4G_CRITICO_NOME_DO_SU','4G_ALERTA_NOME_DO_SU','4G_BOM_NOME_DO_SU']
    for i in cleanInt:
      frameSI[i] = frameSI[i].str.split(',').str[0]

    lista1 = frameSI.columns
    for j in lista1:
      frameSI[j] = [x.replace(',', '.') for x in frameSI[j]]


    Numbers = ['ANF','2G_ACD_NOME_DO_SU','2G_DROP_VOZ_NOME_DO_SU','2G_DROP_DADOS_NOME_DO_SU','2G_DISP_NOME_DO_SU','2G_THROU_USER_NOME_DO_SU','2G_ACV_NOME_DO_SU','2G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_NOME_DO_SU','2G_TRAFEGO_VOZ_TIM_NOME_DO_SU','2G_ACD_(m)NOME_DO_SU','2G_DROP_VOZ_(m)NOME_DO_SU','2G_DROP_DADOS_(m)NOME_DO_SU','2G_DISP_(m)NOME_DO_SU','2G_THROU_USER_(m)NOME_DO_SU','2G_ACV_(m)NOME_DO_SU','2G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_(m)NOME_DO_SU','2G_TRAFEGO_VOZ_TIM_(m)NOME_DO_SU','3G_ACD_NOME_DO_SU','3G_DROP_VOZ_NOME_DO_SU','3G_DROP_DADOS_NOME_DO_SU','3G_DISP_NOME_DO_SU','3G_THROU_USER_NOME_DO_SU','3G_ACV_NOME_DO_SU','3G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_NOME_DO_SU','3G_TRAFEGO_VOZ_TIM_NOME_DO_SU','3G_ACD_(m)NOME_DO_SU','3G_DROP_VOZ_(m)NOME_DO_SU','3G_DROP_DADOS_(m)NOME_DO_SU','3G_DISP_(m)NOME_DO_SU','3G_THROU_USER_(m)NOME_DO_SU','3G_ACV_(m)NOME_DO_SU','3G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_(m)NOME_DO_SU','3G_TRAFEGO_VOZ_TIM_(m)NOME_DO_SU','4G_ACD_NOME_DO_SU','4G_DROP_VOZ_NOME_DO_SU','4G_DROP_DADOS_NOME_DO_SU','4G_DISP_NOME_DO_SU','4G_THROU_USER_NOME_DO_SU','4G_ACV_NOME_DO_SU','4G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_NOME_DO_SU','4G_TRAFEGO_VOZ_TIM_NOME_DO_SU','4G_ACD_(m)NOME_DO_SU','4G_DROP_VOZ_(m)NOME_DO_SU','4G_DROP_DADOS_(m)NOME_DO_SU','4G_DISP_(m)NOME_DO_SU','4G_THROU_USER_(m)NOME_DO_SU','4G_ACV_(m)NOME_DO_SU','4G_VOLUME_DADOS_DLUL_ALLOP(Mbyte)_(m)NOME_DO_SU','4G_TRAFEGO_VOZ_TIM_(m)NOME_DO_SU','4G_CRITICO_NOME_DO_SU','4G_ALERTA_NOME_DO_SU','4G_BOM_NOME_DO_SU','4G_OCUPACAO_NOME_DO_SU','OffLoad4G','OffLoad4G_(m)','OffLoad4G_Voz','OffLoad4G_Voz(m)']
    frameSI[Numbers] = frameSI[Numbers].apply(pd.to_numeric, errors='coerce')

    frameSI.to_excel(csv_path,engine='xlsxwriter',index=False)
    #frameSI.to_csv(csv_path,index=False,header=True,sep=';')

#Baixar arquivo como csv, alterAR CODIGO