import MS_2G
import MS_3G
import MS_4G
import MS_OCUPACAO
import MS

import First
import Second
import WEEK_OFENSORES_Ocupacao
import Ofensor2
import WEEK_PBIConfig

#WEEK_PBIConfig.processArchive()


MS_2G.processArchive()
MS_3G.processArchive()
MS_4G.processArchive()
MS_OCUPACAO.processArchive()
MS.processArchive()


First.processArchive('NOME_DO_SU','Semana')
Second.processArchive('NOME_DO_SU','Semana')

#First.processArchive('Municipio','Semana')
#Second.processArchive('Municipio','Semana')



#WEEK_OFENSORES_Ocupacao.processArchive('NOME_DO_SU')
#Ofensor2.processArchive('Municipio')


#PBI
#Executar depois de carregar a informaçõa no arquivo xlsx
WEEK_PBIConfig.processArchive()
