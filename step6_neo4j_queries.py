from py2neo import *
import pandas as pd
import time

def time_convert(sec,i):
    mins = sec//60
    sec = sec%60
    hrs = mins//60
    print("Time taken by query ",i," = {0}:{1}:{2}".format(int(hrs),int(mins),sec))

graph = Graph("http://localhost:7474", user="username", password="password")

#Query 1
start1 = time.time()
analysis1 = graph.run("match (s:Status),(p:Patient),(g:Gender) where (p)-[:has_status]->(s) and s.stype='Hospitalized'  and (p)-[:is_of_gender]->(g) return g.gtype as Gender,count(p.id) as Total order by count(p.id) Desc limit 1").to_data_frame();
end1 = time.time()
lapsed1 = end1 - start1
time_convert(lapsed1,1)


#Query 2
Gender = analysis1['Gender'];
count = analysis1['Total'];

start1 = time.time()
analysis2 = graph.evaluate("match (s:Status),(p:Patient),(g:Gender) where (p)-[:has_status]->(s) and s.stype='Recovered'  and (p)-[:is_of_gender]->(g) return max(g.gtype)");
end1 = time.time()
lapsed1 = end1 - start1
time_convert(lapsed1,2)


#Query 3
start1 = time.time()
analysis3 = graph.evaluate("match (s:Status),(p:Patient),(g:Gender) where (p)-[:has_status]->(s) and s.stype='Deceased'  and (p)-[:is_of_gender]->(g) return max(g.gtype)");
end1 = time.time()
lapsed1 = end1 - start1
time_convert(lapsed1,3)


#Query 4
start1 = time.time()
analysis4 = graph.evaluate("match (s:Status),(p:Patient),(a:Age) where (p)-[:has_status]->(s) and s.stype='Recovered'  and (p)-[:is_of_age_category]->(a) return  max(a.category)");
end1 = time.time()
lapsed1 = end1 - start1
time_convert(lapsed1,4)


#Query 5
start1 = time.time()
analysis5 = graph.run("match (s:Status),(p:Patient),(a:Age) where (p)-[:has_status]->(s) and s.stype='Deceased'  and (p)-[:is_of_age_category]->(a) return  a.category as Age,count(p.id) as Total1 order by count(p.id) Desc limit 1").to_data_frame();
end1 = time.time()
lapsed1 = end1 - start1
time_convert(lapsed1,5)


#Query 6
Age = analysis5['Age'];
count1 = analysis5['Total1'];

start1 = time.time()
analysis6=graph.run("match (s:State),(p:Patient),(t:Travelled_History) where (p)-[:lives_in]->(s)  and (p)-[:has_travelled_history]->(t) return s.sname as State,count(p.id) as Total2 order by count(p.id) Desc limit 1").to_data_frame();
end1 = time.time()
lapsed1 = end1 - start1
time_convert(lapsed1,6)

#Query 7
State = analysis6['State'];
count2 = analysis6['Total2'];

start1 = time.time()
analysis7=graph.evaluate("MATCH p=()-[r:has_contact_history]->() RETURN count(p)");
end1 = time.time()
lapsed1 = end1 - start1
time_convert(lapsed1,7)


#Query 8
start1 = time.time()
analysis8=graph.evaluate("match (s:Status),(p:Patient) where (p)-[:has_status]->(s) and s.stype='Deceased'  return count(p.id)");
end1 = time.time()
lapsed1 = end1 - start1
time_convert(lapsed1,8)


#Query 9
start1 = time.time()
analysis9=graph.evaluate("match (s:Status),(p:Patient) where (p)-[:has_status]->(s) and s.stype='Recovered'  return count(p.id)");
end1 = time.time()
lapsed1 = end1 - start1
time_convert(lapsed1,9)


print("\n1) Gender which is affected the most with hospitalized status is ",Gender[0]," with total count of ",count[0] );
print("\n2) Gender which is affected the most with recovered status is  ",analysis2 );
print("\n3) Gender which is affected the most with deceased status is  ",analysis3 );
print("\n4) Age group which are recovered the most is ",analysis4 );
print("\n5) Age group which are deceased the most is ",Age[0]," with total count of ",count1[0] );
print("\n6) State that has the most travelled history is ",State[0],"with total count of",count2[0]);
print("\n7) Total number of patients that have come in contact with covid affected patients are  ",analysis7 );
print("\n8,9)Ratio of recovered and deceased patients is ",analysis9,"/",analysis8 );


"""OUTPUT 1:
Time taken by query  1  = 0:0:1.9855091571807861
Time taken by query  2  = 0:0:1.0023062229156494
Time taken by query  3  = 0:0:0.817859411239624
Time taken by query  4  = 0:0:1.6440746784210205
Time taken by query  5  = 0:0:1.5081756114959717
Time taken by query  6  = 0:0:15.296469688415527
Time taken by query  7  = 0:0:0.23195815086364746
Time taken by query  8  = 0:0:0.25096750259399414
Time taken by query  9  = 0:0:0.20964717864990234

1)Gender which is affected the most with hospitalized status is  M  with total count of  33561

2)Gender which is affected the most with recovered status are   M

3)Gender which is affected the most with deceased status are   M

4)Age group which are recovered the most is Young

5)Age group which are deceased the most is  Old  with total count of  792

6)State that has the most travelled history is  Karnataka with total count of 10329

7)Total number of patients that have come in contact with covid affected patients are   14940

8,9)Ratio of recovered and deceased pateints is  507 / 1662


******************************************
OUTPUT 2
Time taken by query  1  = 0:0:0.715214729309082
Time taken by query  2  = 0:0:0.3245522975921631
Time taken by query  3  = 0:0:0.3349301815032959
Time taken by query  4  = 0:0:0.6004712581634521
Time taken by query  5  = 0:0:0.6155474185943604
Time taken by query  6  = 0:0:5.432098388671875
Time taken by query  7  = 0:0:0.07410454750061035
Time taken by query  8  = 0:0:0.08275914192199707
Time taken by query  9  = 0:0:0.08247876167297363

1)Gender which is affected the most with hospitalized status is  M  with total count of  33561

2)Gender which is affected the most with recovered status is   M

3)Gender which is affected the most with deceased status is   M

4)Age group which are recovered the most is  Young

5)Age group which are deceased the most is  Old  with total count of  792

6)State that has the most travelled history is  Karnataka with total count of 10329

7)Total number of patients that have come in contact with covid affected patients are   14940

8,9)Ratio of recovered and deceased patients is  507 / 1662

"""
