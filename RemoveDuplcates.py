import pandas as pd






def processarchive(DataFrame,col_ref,col_sort):
  DataFrame = DataFrame.sort_values([col_sort], ascending = [False])
  DataFrame = DataFrame.drop_duplicates(subset=col_ref, keep="first")#last
  DataFrame = DataFrame.reset_index(drop=True)
  return DataFrame




