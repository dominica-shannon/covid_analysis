# Drop the columns that are not needed
import pandas as pd
import os

entries = os.listdir('Raw Dataset/')

for i in entries:
    fname = "Raw Dataset/"+i
    df = pd.read_csv(fname)
    res = df.loc[:, df.columns.isin(['Date Announced','Age Bracket','Gender','Detected State','Current Status','Notes'])]
    res_fname ='Final Dataset/'+i
    res.to_csv(res_fname, index = False)
