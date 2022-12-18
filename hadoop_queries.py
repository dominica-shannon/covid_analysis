import pandas as pd
import pyarrow as pa
import time

fs=pa.hdfs.connect('localhost',9000)

df=pd.read_csv(fs.open('/project/Filter_dataset.csv'))

def time_convert(sec,i):
    mins = sec//60
    sec = sec%60
    hrs = mins//60
    print("Time taken by query ",i," = {0}:{1}:{2}".format(int(hrs),int(mins),sec))


#Query1 
start1=time.time()
output=df[df['Status']== 'Hospitalized'].groupby(['Gender','Status']).size().reset_index(name='count')
print(output)
end1=time.time()
lapsed1=end1-start1
time_convert(lapsed1,1)
print('\n\n')


#Query2
start1=time.time()
out=df[df['Status']== 'Recovered'].groupby(['Gender','Status']).size().reset_index(name='count')
print(out)
end1=time.time()
lapsed1=end1-start1
time_convert(lapsed1,2)
print('\n\n')


#Query 3
start1=time.time()
ou=df[df['Status']== 'Deceased'].groupby(['Gender','Status']).size().reset_index(name='count')
print(ou)
print(output)
end1=time.time()
lapsed1=end1-start1
time_convert(lapsed1,3)
print('\n\n')


#Query 4
start1=time.time()
Affected=df[df['Status']=='Deceased'].groupby(['Status','Age'],sort=False).size().reset_index(name='Count')
print (Affected.max())
end1=time.time()
lapsed1=end1-start1
time_convert(lapsed1,4)
print('\n\n')


#Query5
start1=time.time()
Recovered=df[df['Status']=='Recovered'].groupby(['Status','Age'],sort=False).size().reset_index(name='Count')
end1=time.time()
print(Recovered.max())
lapsed1=end1-start1
time_convert(lapsed1,5)
print('\n\n')


#Query6
start1=time.time()
output=df.groupby(['State','Travelled']).size().reset_index(name='Count')
q3=output.loc[(output['Count']==output['Count']) & (output['Travelled']=="yes"),['State','Count']]
print(q3[q3.Count==q3.Count.max()])
end1=time.time()
lapsed1=end1-start1
time_convert(lapsed1,6)
print('\n\n')


#Query 7
start1=time.time()
output= df['Contact'].tolist().count('yes')
print(output)
end1=time.time()
lapsed1=end1-start1
time_convert(lapsed1,7)
print('\n\n')


#Query8
start1=time.time()
output=df.groupby(['Status']).size().reset_index(name='Count')
q3=output.loc[(output['Count']==output['Count']) & (output['Status']=="Deceased"),['Status','Count']] 
print(q3)
end1=time.time()
lapsed1=end1-start1
time_convert(lapsed1,8)
print('\n\n')


#Query9
start1=time.time()
output=df.groupby(['Status']).size().reset_index(name='Count')
q4=output.loc[(output['Count']==output['Count']) & (output['Status']=="Recovered"),['Status','Count']]
print(q4)
end1=time.time()
lapsed1=end1-start1
time_convert(lapsed1,9)
print('\n\n')

