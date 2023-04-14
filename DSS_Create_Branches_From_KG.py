from SPARQLWrapper import GET, JSON, JSONLD, POST, TURTLE, SPARQLWrapper
import sparql_dataframe
import yaml
import pandas as pd
import os
import uuid
import json


endpoint = "http://localhost:7200/repositories/onto-rdf-1410"

class SRO_Extract:

    def __init__(self):
        self.df_class_dp = pd.DataFrame(columns=['Subject', 'Relation', 'Object'])
        self.count = 0 
        self.inst_list = []
        self.class_list = []
        self.done_class_list = []
        self.branch = ''
        self.branch_count = 0
        self.list_all_branches = []
        self.dict_conversation = {}

        self.test_branches = ['SW_Eligibility_PBS -> SW_Eligibility_PBS_Sponsorship -> SW_Job_Genuineness',
                            'SW_Eligibility_PBS -> SW_Eligibility_PBS_Sponsorship -> SW_Valid_Sponsor -> SW_Job_Genuineness',
                            'SW_Eligibility_PBS -> SW_Eligibility_PBS_Sponsorship -> SW_CoS',
                            'SW_Eligibility_PBS -> SW_Eligibility_PBS_Sponsorship -> SW_Job_Offer -> SW_CoS']
        
        self.dict_test_branches = {'SW_Eligibility_PBS -> SW_Eligibility_PBS_Sponsorship -> SW_Job_Offer -> SW_CoS' : ['Does the applicant has a valid job offer?', 'Is the sponsor providing a valid CoS as per the requirement of the job offer?'],
            'SW_Eligibility_PBS -> SW_Eligibility_PBS_Sponsorship -> SW_Job_Genuineness' : ['Is the requirement of the Immigration Skills Charge being paid in full by the sponsor fulfilled in the sponsorship obtained from a valid sponsor?', 'Is the sponsor an A-rated sponsor under the Home Office unless the applicant was previously granted a Skilled Worker visa under the same sponsor?'],
            'SW_Eligibility_PBS -> SW_Eligibility_PBS_Sponsorship -> SW_CoS' : ['Does the valid Certificate of Sponsorship (CoS) from a valid sponsor contain PAYE details as required by the HM Revenue and Customs (HMRC) for the sponsored job?'],
            'SW_Eligibility_PBS -> SW_Eligibility_PBS_Sponsorship -> SW_Valid_Sponsor -> SW_Job_Genuineness' : ['Is the sponsor\'s genuine need for the job and the sponsor\'s genuine type of business met in this sponsorship?', 'Is the sponsor\'s history of compliance with the immigration system good, and is the applicant\'s skill set, qualifications, and experience appropriate for the offered job in this sponsorship?']
        }

        query1 = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                PREFIX : <http://dev.org/ImmigrateUK.owl#>
                select DISTINCT ?s where { 
                    ?s ?p ?o .
                    ?s rdf:type owl:Class
                } """
    

        df1 = sparql_dataframe.get(endpoint, query1, post=True)
        if(len(df1) > 0):
            self.class_list = df1['s'].values.tolist()
            # print(self.class_list)
    
    def extractOPWithDP(self, inst_uri, class_uri):
        self.branch += ' -> ' + str(inst_uri) 
        # print('branch made in extractOPWithDP : ', self.branch)
        query = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX owl: <http://www.w3.org/2002/07/owl#>
            PREFIX : <http://dev.org/ImmigrateUK.owl#>            
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            select ?sub ?r ?oo ?o where {
            { select ?sub ?r ?o where 
                { 
                        values ?s {:""" + inst_uri + """}
                        ?s ?p ?o .
                        ?s :hasDefinition ?sub.
                        ?p rdf:type ?dop.
                        ?p rdfs:label ?r
                        FILTER (?p NOT IN (:hasDefinition) && ?dop in (owl:DatatypeProperty))
                }
            } UNION
            {
            select ?sub ?r ?oo ?o where 
                { 
                        values ?s {:""" + inst_uri + """}
                        ?s ?p ?oo .
                        ?s :hasDefinition ?sub.
                        ?p rdf:type ?dop.
                        ?p rdfs:label ?r.
                        ?oo :hasDefinition ?o
                        FILTER (?p NOT IN (:hasDefinition) && ?dop in (owl:ObjectProperty))
                }
            }
            }"""
        
        df = sparql_dataframe.get(endpoint, query, post=True)
        if(len(df) > 0):
            for i in range(0, len(df)):
                # print('inside extractOPWithDP')
            
                if(str(df.iloc[i]['oo']).startswith('http://dev.org/ImmigrateUK.owl') and  df.iloc[i]['oo'] not in self.class_list):
                    # and df.iloc[i]['oo'][31:] not in self.inst_list):
                    # print(' calling ', df.iloc[i]['oo'][31:], ' from extractOPWithDP if')
                    self.df_class_dp.loc[self.count] = [df.iloc[i]['sub'], df.iloc[i]['r'], df.iloc[i]['o']]
                    self.count += 1
                    self.inst_list.append(df.iloc[i]['oo'][31:])
                    self.extractOPWithDP(df.iloc[i]['oo'][31:], class_uri)
                
                elif(df.iloc[i]['oo'] in self.class_list):
                    # and df.iloc[i]['oo'][31:] not in self.done_class_list):
                    self.df_class_dp.loc[self.count] = [df.iloc[i]['sub'], df.iloc[i]['r'], df.iloc[i]['o']]
                    self.count += 1
                    # print(' calling class ', df.iloc[i]['oo'][31:], ' from extractOPWithDP else')
                    self.done_class_list.append(df.iloc[i]['oo'][31:])
                    # self.branch_count = 0
                    self.extractClassInstances(df.iloc[i]['oo'][31:])
                
                else:
                    # print('in else of extractOPWithDP : ', [df.iloc[i]['sub'], df.iloc[i]['r'], df.iloc[i]['o']])
                    self.df_class_dp.loc[self.count] = [df.iloc[i]['sub'], df.iloc[i]['r'], df.iloc[i]['o']]
                    self.count += 1
                
                # self.done_class_list.append(':' + class_uri)


            # print(self.df_class_dp.head())


    def extractClassInstances(self, class_uri):
        query =  """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                    PREFIX owl: <http://www.w3.org/2002/07/owl#>
                    PREFIX : <http://dev.org/ImmigrateUK.owl#>            
                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    select ?sub ?s ?r ?oo ?o ?def where {
                    { select ?sub ?s ?r ?o ?def where { 
                                ?s ?p ?o.
                                ?s rdf:type :""" + class_uri + """.
                                ?s :hasDefinition ?sub.
                                ?p rdf:type ?dop.
                                ?p rdfs:label ?r.
                                # ?oo :hasDefinition ?o.
                                :""" + class_uri + """ rdfs:isDefinedBy ?def
                                FILTER (?p NOT IN (:hasDefinition) && ?dop in (owl:DatatypeProperty))
                            }
                    } UNION
                    {
                    select ?sub ?s ?r ?o ?oo ?def where { 
                                ?s ?p ?o.
                                ?s rdf:type :""" + class_uri + """.
                                ?s :hasDefinition ?sub.
                                ?p rdf:type ?dop.
                                ?p rdfs:label ?r.
                                ?o :hasDefinition ?oo.
                                :""" + class_uri + """ rdfs:isDefinedBy ?def
                                FILTER (?p NOT IN (:hasDefinition) && ?dop in (owl:ObjectProperty))
                            }
                    }
                    }"""

        df = sparql_dataframe.get(endpoint, query, post=True)
        if(len(df) > 0):
            print(df.head(10))
            for i in range(0, len(df)):
                    print('inside extractClassInstances')
                    print(df.iloc[i]['s'][31:], str(df.iloc[i]['o']))
                    print()
                # if(df.iloc[i]['s'][31:] == 'SW_Eligibility_PBS_Sponsorship'):
                    if(str(df.iloc[i]['o']).startswith('http://dev.org/ImmigrateUK.owl') and df.iloc[i]['o'] not in self.class_list and df.iloc[i]['o'][31:] not in self.inst_list):
                        print(' calling ', df.iloc[i]['o'][31:], ' from extractClassInstances if')
                        self.df_class_dp.loc[self.count] = [df.iloc[i]['sub'] + " falls under " + df.iloc[i]['def'] + ' and ', df.iloc[i]['r'], df.iloc[i]['oo']]
                        self.count += 1
                        self.inst_list.append(df.iloc[i]['o'][31:])
                        self.branch += class_uri + ' -> ' + df.iloc[i]['s'][31:]
                        self.extractOPWithDP(df.iloc[i]['o'][31:], class_uri)
                        print('Branch made : ', self.branch)
                        # print('Branch count : ', self.branch_count)
                        self.list_all_branches.append(self.branch)

                        '''if(self.branch in self.test_branches):
                            # print(self.dict_test_branches[self.branch])
                            for q in self.dict_test_branches[self.branch]:
                                print('LILA : ', q)
                                ans = input('User (Please answer in yes or no): ')
                                self.dict_conversation[q] = ans

                                print()'''

                        self.branch_count = 0
                        self.branch = ''
                    
                    elif(df.iloc[i]['o'] in self.class_list and df.iloc[i]['o'][31:] not in self.done_class_list):
                        print(' calling class ', df.iloc[i]['o'][31:], ' from extractClassInstances else ')
                        self.df_class_dp.loc[self.count] = [df.iloc[i]['sub'] + " falls under " + df.iloc[i]['def'] + ' and ', df.iloc[i]['r'], df.iloc[i]['oo']]
                        self.count += 1
                        self.done_class_list.append(df.iloc[i]['o'][31:])
                        self.branch += class_uri + ' -> ' + df.iloc[i]['s'][31:]
                        self.extractClassInstances(df.iloc[i]['o'][31:])
                        # print('Branch made : ', self.branch)
                        # print('Branch count : ', self.branch_count)
                        self.list_all_branches.append(self.branch)
                        self.branch_count = 0
                        self.branch = ''
                    
                    else:
                        print('in else of extractClassInstances : ', [df.iloc[i]['sub'] + " falls under " + df.iloc[i]['def'] + ' and ', df.iloc[i]['r'], df.iloc[i]['o']])
                        self.df_class_dp.loc[self.count] = [df.iloc[i]['sub'] + " falls under " + df.iloc[i]['def'] + ' and ', df.iloc[i]['r'], df.iloc[i]['o']]
                        self.count += 1

                    
                    self.done_class_list.append(':' + class_uri)
                    

            # print(self.df_class_dp.head())
obj = SRO_Extract()

'''for c in obj.class_list:
    print(c[31:])
    # http://dev.org/ImmigrateUK.owl#ELP_SELT_Providers
    obj.extractClassInstances(c[31:] )'''
obj.extractClassInstances('SW_Eligibility')

print(obj.dict_conversation)

'''obj.df_class_dp.to_csv('pbs_2.csv')
print(obj.inst_list)
print(obj.done_class_list)
print(len(obj.done_class_list))
print(obj.list_all_branches)
print(len(obj.list_all_branches))'''
df = pd.DataFrame(obj.list_all_branches,columns=['branches'])
print(df)
df.to_csv('branch_skilled_worker.csv')

