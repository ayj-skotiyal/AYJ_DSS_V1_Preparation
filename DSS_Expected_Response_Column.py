import yaml
import pandas as pd
import os
import uuid
import json
import openai

openai.api_key = "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"
df_temp = pd.DataFrame(columns=['Leaf_Node', 'Facts', 'Questions','Expected_Answer'])
df = pd.read_csv('df_questions7iii.csv', encoding= 'unicode_escape')
question = ''
for i in range(0, len(df)):
    question = 'What should be the answer to the question "'+ df.iloc[i]['Questions'] +'" based on the fact "' + df.iloc[i]['Facts']+ '" ? Please answer in one word : a "yes" or "no".'
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
    # print('response : ', response.choices[0].text)
    response = (response.choices[0].text).strip()

    df_temp.loc[i] = [df.iloc[i]['Leaf_Node'], df.iloc[i]['Facts'], df.iloc[i]['Questions'], response]
    print('Fact : ', df.iloc[i]['Facts'], ' Question : ', df.iloc[i]['Questions'], ' Response : ', response)

df_temp.to_csv('df_questions7iii_SK_2.csv')