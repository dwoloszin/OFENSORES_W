import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date

def processArchive():
    fields = ['END_ID','LOCAL','ATENDIMENTO','Detentora','Observações']
    fields2 = ['END_ID','LOCAL','ATENDIMENTO','Detentora','Obs']
    foltherName = 'INVENTARIO'
    pathImport = '/import/'+foltherName
    pathImportSI = os.getcwd() + pathImport
    #print (pathImportSI)
    archiveName = pathImport[8:len(pathImport)]
    #print (archiveName)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    csv_path = os.path.join(script_dir, 'export/'+foltherName+'/'+archiveName+'.csv')
    #print ('loalding files...\n')
    all_filesSI = glob.glob(pathImportSI + "/*.xlsx")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    #print (all_filesSI)
    li = []
    df = pd.DataFrame()
    for filename in all_filesSI:
        #data = pd.read_excel(filename,skiprows=27,sheet_name = 'DUMP_5G_DSS', nrows=40,usecols = 'A:AC')
        data = pd.read_excel(filename,sheet_name = 'Sites Indoor',usecols = fields)
        frameSI = df.append(data,ignore_index=True)
        frameSI = frameSI[fields] # ordering labels 
    frameSI.columns = fields2


    frameSI = frameSI.drop_duplicates()
    frameSI = frameSI.reset_index(drop=True)
    frameSI.to_csv(csv_path,index=True,header=True,sep=';')
    return frameSI
