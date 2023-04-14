from SPARQLWrapper import GET, JSON, JSONLD, POST, TURTLE, SPARQLWrapper
import sparql_dataframe
import yaml
import pandas as pd
import os
import uuid
import json
# import DSS_V1_Request_Respond_GPT3


endpoint = "http://localhost:7200/repositories/Onto-1502"

df = pd.read_csv('result_final7.csv', encoding= 'unicode_escape')

set_class = set(df['Class'])
set_category = set(df['Category'])

print(set_class)

print(set_category)
def defineEntity(entity):
    query = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX owl: <http://www.w3.org/2002/07/owl#>
            PREFIX : <http://dev.org/ImmigrateUK.owl#>
            select  * where { 
                # :SW_Applicant_Suitability :hasDefinition ?o.
                :"""+ entity +""" :hasDefinition ?o.
                    
            } """
        

    df = sparql_dataframe.get(endpoint, query, post=True)
    print(df)
    return_var = entity
    if(len(df) > 0):
        return_var = df.iloc[0]['o']

    print(return_var)
    return return_var


# for i in range(0, len(df)):

'''for i in set_class:
    for j in set_category:
        for k in range(0, len(df)):
            if(df.iloc[k]['Class'] == i):
                print(i)
                print(df.loc[k])'''

'''df_temp = pd.DataFrame(columns=['Fact', 'Branch_Length', 'Leaf_Node', 'Branch'])
df_temp_2 = pd.DataFrame(columns=['Fact', 'Branch_Length', 'Leaf_Node', 'Branch'])'''

df_temp = pd.DataFrame(columns=['Branch_Length', 'Leaf_Node', 'Branch'])
df_temp_2 = pd.DataFrame(columns=['Branch_Length', 'Leaf_Node', 'Branch'])
temp = pd.DataFrame(columns=['facts', 'branch'])
dict_temp = {}
dict_temp_branches = {}
list_traversed_instances = []
fact = ""
df_temp_branches = pd.DataFrame(columns=['Class', 'Category', 'Branch', 'Leaf_Node', 'Branch_Length', 'Leaf_Node_Definition', 'Class_Definition', 'Category_Definition', 'Requirement_Level'])
# Class_Definition	Category_Definition	Requirement_Level

temp_list = []
count = 0
c = 0
for i in set_class:
    for j in set_category:
        for k in range(0, len(df)):
            if(df.iloc[k]['Class'] == i and df.iloc[k]['Category'] == j):
                print(k)
                temp_l_n = ""
                if(i + ', ' + j + ', ' + df.iloc[k]['Branch'] + ', ' + df.iloc[k]['Leaf_Node'] + ', ' + str(df.iloc[k]['Branch_Length']) not in temp_list):
                    temp_list.append(i + ', ' + j + ', ' + df.iloc[k]['Branch'] + ', ' + df.iloc[k]['Leaf_Node'] + ', ' + str(df.iloc[k]['Branch_Length']))
                    if('(' in df.iloc[k]['Leaf_Node']):
                        temp_l_n = defineEntity(df.iloc[k]['Leaf_Node'].split('(')[0])
                    else:
                        temp_l_n = defineEntity(df.iloc[k]['Leaf_Node'])
                    # temp_l_n = defineEntity(df.iloc[k]['Leaf_Node'])
                    df_temp_branches.loc[c] = [i, j, df.iloc[k]['Branch'], df.iloc[k]['Leaf_Node'], df.iloc[k]['Branch_Length'], temp_l_n, df.iloc[k]['Class_Definition'], df.iloc[k]['Category_Definition'], df.iloc[k]['Requirement_Level']]
                    c += 1

                if(df.iloc[k]['Leaf_Node'] + '~ ' + df.iloc[k]['Relation'] not in list(dict_temp.keys())):
                    dict_temp[df.iloc[k]['Leaf_Node'] + '~ ' + df.iloc[k]['Relation']] = [df.iloc[k]['Subject'] + ' ' + df.iloc[k]['Relation'], ' ' + df.iloc[k]['Object']]
                else:
                    if(' ' + df.iloc[k]['Object'] not in dict_temp[df.iloc[k]['Leaf_Node'] + '~ ' + df.iloc[k]['Relation']]):
                        dict_temp[df.iloc[k]['Leaf_Node'] + '~ ' + df.iloc[k]['Relation']].append(' ' + df.iloc[k]['Object'])

                '''if(df.iloc[k]['Branch'] + ' ~ ' + str(df.iloc[k]['Branch_Length']) not in list(dict_temp_branches.keys())):
                    dict_temp_branches[df.iloc[k]['Branch'] + ' ~ ' + str(df.iloc[k]['Branch_Length'])] = [df.iloc[k]['Class'], df.iloc[k]['Category']]
               '''

                '''df_temp.loc[count] = [df.iloc[k]['Branch_Length'], df.iloc[k]['Leaf_Node'], df.iloc[k]['Branch'].split(' -> ')]
                # df_temp_2 = df_temp.sort_values(by=["Branch_Length"], ascending=False) 
                df_temp_2 = df_temp.sort_values(by=["Branch_Length"], ascending=False).reset_index(drop=True)
                count += 1'''


                '''for m in range(0, len(df_temp_2)):
                    print(df_temp_2.loc[m]['Branch'])
                    for n in df_temp_2.loc[m]['Branch']:
                        print(n)
                        if(n not in list_traversed_instances):
                            for k, v in dict_temp.items():
                                # print('k.split(~) : ', k.split('~'))
                                if(n in k.split('~') and n not in list_traversed_instances):
                                    for v1 in v:
                                        print('v1 : ', v1)
                                        fact += v1
                                    fact += '. '
                                    print(n, ' : ', fact)
                                    temp.loc[c] = [n + ' : ' + fact, df_temp_2.loc[m]['Branch']]
                                    c += 1
                                fact = ""
                        list_traversed_instances.append(n)
'''
            
print(dict_temp)
# temp.to_csv('temp2.csv')
# print(dict_temp_branches)
df_temp_branches.to_csv('df_temp_branches7iii.csv')


df_facts = pd.DataFrame(columns=['Leaf_Node', 'Facts'])
count = 0
fact = ""
for k, v in dict_temp.items():
    print(k, ' : ', v)

    fact += v[0] + v[1]
    for v1 in range(2, len(v)):
        fact += ', ' + v[v1]
    fact += '. '
    print('Fact : ', fact)
    # print(k, k.split('~')[0])
    '''if('(' in k.split('~')[0]):
        temp_l_n = k.split('~')[0].split('(')[0]
    else:
        temp_l_n = defineEntity(k.split('~')[0])'''
    df_facts.loc[count] = [k.split('~')[0], fact]
    count += 1
    fact = ""
    print(k.split('~')[0], ' : ', fact)

print(df_facts.head(10))
df_facts.to_csv('df_facts7iii.csv')