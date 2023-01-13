import timeit

import MS_2G
import MS_3G
import MS_4G
import MS_OCUPACAO
import MS




print ('\nprocessing... ')
inicio = timeit.default_timer()

MS_2G.processArchive()
MS_3G.processArchive()
MS_4G.processArchive()
MS_OCUPACAO.processArchive()
MS.processArchive()







fim = timeit.default_timer()
print ('duracao: %.2f' % ((fim - inicio)/60) + ' min') 
