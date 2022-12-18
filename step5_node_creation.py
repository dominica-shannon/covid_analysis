from py2neo import *

graph = Graph("http://localhost:7474", user="username", password="password")
clr_neo = graph.run("match(n) detach delete n")

query1 = """
        USING PERIODIC COMMIT
        LOAD CSV WITH HEADERS FROM 
        'file:///D:/Covid/Filter_dataset.csv' AS row
        WITH row where row.Travelled='yes'
        MERGE(t:Travelled_History{reason:'Had travel history'})

        MERGE(p:Patient{id:toInteger(row.Patient_Id)})
        CREATE (p)-[:has_travelled_history]->(t)
        """

query2 = """
        USING PERIODIC COMMIT
        LOAD CSV WITH HEADERS FROM 
        'file:///D:/Covid/Filter_dataset.csv' AS row

        WITH row where row.Contact='yes'
        MERGE(c:Contact_History{reason:'Came in contact with covid positive'})

        MERGE(p:Patient{id:toInteger(row.Patient_Id)})
        CREATE (p)-[:has_contact_history]->(c)
        """

query3 = """
        USING PERIODIC COMMIT
        LOAD CSV WITH HEADERS FROM 
        'file:///D:/Covid/Filter_dataset.csv' AS row

        WITH row where row.Disease='yes'
        MERGE(d:Disease_History{reason:'Had some health issues'})

        MERGE(p:Patient{id:toInteger(row.Patient_Id)})
        CREATE (p)-[:has_health_history]->(d)
        """

query4 = """
        USING PERIODIC COMMIT
        LOAD CSV WITH HEADERS FROM 
        'file:///D:/Covid/Filter_dataset.csv' AS row

        MERGE (g:Gender {gtype:row.Gender})
        MERGE (p:Patient{id:toInteger(row.Patient_Id)})  
        CREATE (p)-[:is_of_gender]->(g)
        """

query5 = """
        USING PERIODIC COMMIT
        LOAD CSV WITH HEADERS FROM 
        'file:///D:/Covid/Filter_dataset.csv' AS row
        
        WITH row,
        (CASE row.Age WHEN 'Child' THEN 'Child' 
        WHEN 'Teen' THEN 'Teen' 
        WHEN 'Young' THEN 'Young' 
        WHEN 'Mid age' THEN 'Mid' 
        WHEN 'Old age' THEN 'Old' 
        ELSE 'Other' END) AS ctype 
        
        MERGE (a:Age{category:ctype}) 
        MERGE (p:Patient{id:toInteger(row.Patient_Id)})  
        CREATE (p)-[:is_of_age_category]->(a)"""

query6 = """
        USING PERIODIC COMMIT
        
        LOAD CSV WITH HEADERS FROM 
        'file:///D:/Covid/Filter_dataset.csv' AS row
        
        MERGE (s:State {sname:row.State})
        MERGE (p1:Patient {id:toInteger(row.Patient_Id)})    
        CREATE (p1)-[:lives_in]->(s)"""

query7 = """
        USING PERIODIC COMMIT
        LOAD CSV WITH HEADERS FROM 
        'file:///D:/Covid/Filter_dataset.csv' AS row
        
        MERGE (ss:Status {stype:row.Status})
        MERGE (p2:Patient{id:toInteger(row.Patient_Id)})  
        CREATE (p2)-[:has_status]->(ss)"""
        
query8 = """
        USING PERIODIC COMMIT
        LOAD CSV WITH HEADERS FROM 
        'file:///D:/Covid/Filter_dataset.csv' AS row
        
        MERGE (d:Date {date:row.Date})
        MERGE (p4:Patient{id:toInteger(row.Patient_Id)})  
        CREATE (p4)-[:on]->(d)"""

graph.run(query1)
graph.run(query2)
graph.run(query3)
graph.run(query4)
graph.run(query5)
graph.run(query6)
graph.run(query7)
graph.run(query8)
