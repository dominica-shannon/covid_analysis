# Filteration for age
# Removal of rows whose entry is blank for Notes,Age Bracket & Gender
import pandas as pd
import numpy as np

check = ['Contact of under tracing','Details awaited','Contact of under','Contact under tracing','Details Awaited']
data = pd.read_csv("Final Dataset/combined_csv.csv")
df = data.dropna(subset=['Notes','Age Bracket','Gender'],how='any')
df = df[~df['Notes'].isin(check)]

df['Age Bracket'] = df['Age Bracket'].astype(str) #convert age bracket to string
df['Age_Str_Length'] = df['Age Bracket'].str.len() #Calculate length of age bracket

#Find substring "-" in age group & drop it. Eg. 28-35
substring = "-"

for i in df.index:
    if substring in df['Age Bracket'][i]:
        print ("Found!",df['Age Bracket'][i])
        df = df.drop(i)

for i in df.index:
    if (df['Age_Str_Length'][i] > 4):
        res = df['Age Bracket'][i].replace('.', '', 1).isdigit()
        if res == False:
            print(df['Age Bracket'][i])
            df['Age Bracket'][i] = 1

df.insert(loc=0, column='Patient_Id', value=np.arange(len(df)))

df.to_csv("Dataset.csv",index=False)
