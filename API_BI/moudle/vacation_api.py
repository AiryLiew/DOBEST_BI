# -*- coding: utf-8 -*-
# 测试环境: python3.9

import requests
import json
import pandas as pd
from pandas.io.json import json_normalize
from datetime import datetime


list_month = []
for j in range(2022,datetime.now().year+2):# 年度范围调整
    for i in range(1,13):
        date = str(j) + '-' + str(i)
        list_month.append(date)


url = "http://v.juhe.cn/calendar/month?"
key = "844f8dc0f4d6daf5614c8e3ab4fdc7cd"
mes = []
for year_month in list_month:
    
    uri = url + 'year-month=' + year_month + '&key=' + key
    result = requests.get(uri)

    body = result.text
    response = json.loads(body)
    mes.append(response)


df = json_normalize(mes) 
dfisna = df['result.data.holiday_array'].dropna()
dfisna.reset_index(drop=True,inplace=True)

def json_statu(dfisna):  # status 1:假日，2:工作日
    list1 = []
    for i in range(len(dfisna)):
        df1 = json_normalize(dfisna[i])
        list1.append(df1)
    df2 = pd.concat(list1)
    df2.reset_index(drop=True,inplace=True)
    return df2

df1 = json_statu(dfisna)
df2 = json_statu(df1['list'])

with open(r'C:\Users\liujin02\Desktop\forecast-clean\vacation2.json', 'w',encoding='utf-8') as f:
    json.dump(mes, f)
df2.to_excel('vacation2.xlsx',index=False)