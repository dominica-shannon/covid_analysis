import pandas as pd
df = pd.read_csv("Dataset.csv")
travel_match=["travelled from","travelled to","travel history","returned from","visited plce","place returnees","history of travel"]
contact_match=["contact","family members","relative","neighbour","wife","daughter","son","father","spouse","doctor","sister","of p","grandmother,garnd son","uncle","aunt","husband","nurse","roomate","cousin"]
history_match=["chronic","high blood pressure","heart disease","swine flu","acute respiratory infection","SARI","cardiac","cancer","Diabetis","liver","hypertension","tuberculosis","ILI","comorbidities","kidney","asthma","htn","obesity","t2dm","hypothyroidism"]
list1 = []
list2 = []
list3 = []
age_category = []

for i in range(len(df)):
    res_travel=any(matcher in df.loc[i,"Notes"].lower() for matcher in travel_match)
    res_contact=any(matcher in df.loc[i,"Notes"].lower() for matcher in contact_match)
    res_history=any(matcher in df.loc[i,"Notes"].lower() for matcher in history_match)
    if(res_travel):
        list1.append("yes")
    else:
        list1.append("no")

    if(res_contact):
        list2.append("yes")
    else:
        list2.append("no")

    if(res_history):
        list3.append("yes")
    else:
        list3.append("no")

    age = df['Age Bracket'][i]

    if age < 13:
        age_category.append("Child")

    elif age >= 13 and age <= 19:
        age_category.append("Teen")

    elif age >= 20 and age <= 40:
        age_category.append("Young")

    elif age >= 41 and age <= 60:
        age_category.append("Mid age")

    elif age > 60:
        age_category.append("Old age")

df['Age'] = age_category
df['Travelled'] = list1
df['Contact'] = list2
df['Disease'] = list3

df.to_csv("Dataset.csv",index=False)
