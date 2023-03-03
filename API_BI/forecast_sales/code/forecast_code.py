# -*- coding: utf-8 -*-
# 测试环境: python3.10.1

# 将存在其它字符表示分类的特征转化为数字
def dict_(df,name):

    df_wl = df[name].drop_duplicates().reset_index(drop=True)
    df_wl1 = df_wl.reset_index(drop=True).reset_index()
    df_wl1['index'] = df_wl1['index']+1 # 索引+1，后续填充0时防止和有序变量弄混
    return dict(zip(df_wl1[name],df_wl1['index'])), df_wl


# 创建日期辅助表
def create_assist_date(datestart,dateend):

    import pandas as pd
    from datetime import timedelta

    date_list = []
    date_list.append(datestart)
    while datestart < dateend:
        datestart+=timedelta(days=+1)
        date_list.append(datestart)
    return pd.DataFrame(date_list,columns=['riqi'])


def change(df2,df_holiday):

    import pandas as pd
    from datetime import datetime

    #将时间特征进行拆分和转化
    df2['year']=df2['riqi'].apply(lambda x:x.year)
    df2['month']=df2['riqi'].apply(lambda x:x.month)
    df2['day']=df2['riqi'].apply(lambda x:x.day)
        
    df2['IsVacation'] = df2.apply(lambda x:1 if x['month']==1 or x['month']==2 or x['month']==7 or x['month']==8 else 0 ,axis=1)
    # 'IsVacation' ： 0：非假期，1：寒暑假

    df2['weekday'] = df2.apply(lambda x: datetime.weekday(x['riqi'])+1,axis=1)
    # 'weekday' ： 返回中国式星期

    df2 = pd.merge(df2,df_holiday,left_on=['riqi'],right_on=['date'],how='left')
    df2['status'].fillna(0,inplace=True)
    for i in range(len(df2)):
        if df2['status'][i] == 0 and (df2['weekday'][i]==6 or df2['weekday'][i]==7):
            df2['status'][i] = 1
        elif df2['status'][i] != 0:
            continue
        else:
            df2['status'][i] = 2
    # 'status' ： 1：节假日，2：工作日
    df2.drop(['date'],axis=1,inplace=True)
    df2['status'].replace(2,0,inplace=True)
    return df2


def getDictKey(mydict,value,word):
    try:
        return [k for k,v in mydict.items() if value == v][0]
    except:
        return word


# 补日期函数
def datena(df,name,df_holiday): 
    import pandas as pd

    df = df.reset_index(drop=True)
    date_end = df['riqi'].max()
    
    result = []
    for j in range(len(dict_(df,name)[1])):
        df1 = df[df[name]==dict_(df,name)[1][j]]
        date_start = df1['riqi'].min()
        df_date = create_assist_date(date_start,date_end)

        df1 = pd.merge(df_date,df1,on=['riqi'],how='left')
        df1[name].fillna(dict_(df,name)[1][j],inplace=True)
        result.append(df1)
    df2 = pd.concat(result, sort=False)
    df2 = df2.reset_index(drop=True)
    return change(df2,df_holiday), len(df_date)