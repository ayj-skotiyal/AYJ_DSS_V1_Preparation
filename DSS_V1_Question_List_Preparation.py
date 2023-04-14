from SPARQLWrapper import GET, JSON, JSONLD, POST, TURTLE, SPARQLWrapper
import sparql_dataframe
import yaml
import pandas as pd
import os
import uuid
import json
import openai


endpoint = "http://localhost:7200/repositories/Onto-1502"

openai.api_key = "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"

class DSS_GPT3:

    def __init__(self):
        self.done_instance_list = []
        self.dict_requirement_level = {}
        self.branch_list = []

        self.df_facts = pd.read_csv('df_facts7iii.csv', encoding= 'unicode_escape')
        
        self.df_questions = pd.DataFrame(columns=['Leaf_Node', 'Facts', 'Questions'])

        query2 = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX owl: <http://www.w3.org/2002/07/owl#>
                PREFIX : <http://dev.org/ImmigrateUK.owl#>
                select  DISTINCT ?s ?sub ?o ?def where { 
                    ?s ?p ?o .
    				?o rdf:type :Requirement_Level.
                    ?o :hasDefinition ?def.
                    ?s :hasDefinition ?sub
                } """
    

        df2 = sparql_dataframe.get(endpoint, query2, post=True)
        if(len(df2) > 0):
            for  i in range(0, len(df2)):
                self.dict_requirement_level[df2['s'][i]] = str(df2['sub'][i]) + ' is a ' + df2['def'][i]
                
            # print('dict_requirement_level : ', self.dict_requirement_level)
            print()
    

    def checkRequirementLevel(self, inst_uri):
        print(self.dict_requirement_level)
        print()
        if(inst_uri in list(self.dict_requirement_level.keys())):
            print('LILA : ' + self.dict_requirement_level[inst_uri])
    
    def formQuestionsGPT(self):
        for i in range(25, 50): # len(self.df_facts)
            print(i)
            if('(p)' not in self.df_facts.iloc[i]['Leaf_Node'] and '(c)' not in self.df_facts.iloc[i]['Leaf_Node']):
                print(self.df_facts.iloc[i]['Leaf_Node'], ' : ', self.df_facts.iloc[i]['Facts'])

                question = "Ask the applicant a detailed interrogative question to check if the applicant fulfills this requirement and the question should have a yes or no answer '" + self.df_facts.iloc[i]['Facts'] + "'"

                response = openai.Completion.create(
                model="text-davinci-003",
                prompt = question,
                temperature=0.7,
                max_tokens=256,
                top_p=1,
                frequency_penalty=0,
                presence_penalty=0
                )
                # print('response : ', response)
                print()
                print('response : ', response.choices[0].text)
                response = (response.choices[0].text).strip()

                self.df_questions.loc[i] = [self.df_facts.iloc[i]['Leaf_Node'], self.df_facts.iloc[i]['Facts'], response]
            
            else:
                question = "Create a proper sentence for the fact '" + self.df_facts.iloc[i]['Facts'] + "'"

                response = openai.Completion.create(
                model="text-davinci-003",
                prompt = question,
                temperature=0.7,
                max_tokens=256,
                top_p=1,
                frequency_penalty=0,
                presence_penalty=0
                )
                # print('response : ', response)
                print()
                print('response : ', response.choices[0].text)
                response = (response.choices[0].text).strip()

                self.df_questions.loc[i] = [self.df_facts.iloc[i]['Leaf_Node'], self.df_facts.iloc[i]['Facts'], response]




        '''for b in branch:
            self.branch_list = branch.split(' -> ')
            print('self.branch_list : ', self.branch_list)
            print(current_inst)
            i = (self.branch_list).index(current_inst)
            print(i)'''

obj = DSS_GPT3()
obj.formQuestionsGPT()
obj.df_questions.to_csv('df_questionsTemp.csv')