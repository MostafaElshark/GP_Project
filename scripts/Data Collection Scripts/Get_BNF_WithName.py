import pandas as pd

df1 = pd.read_excel(r'F:\GP practice\BNF Snomed Mapping data 20230919.xlsx')
mydf = df1[['BNF Code', 'BNF Name']]
mydf = mydf.drop_duplicates()
mydf = mydf.dropna()
mydf.to_excel('F:\GP practice\product_BNF.xlsx')
