# -*- coding: utf-8 -*-
# 测试环境: python3.10.1

import os
import pandas as pd
import xgboost as xgb
import calendar
from sqlalchemy import create_engine,text
from datetime import datetime
from datetime import timedelta
from sklearn import model_selection

# scikit-learn              1.0.2
from sklearn.model_selection import GridSearchCV


print("\n","START FORECAST_SALES",datetime.now(),"\n")

#设置文件夹路径，获取文件夹下的所有文件名
path =r'C:\Users\liujin02\Desktop\邮件报表\采购订单'

#合并文件夹下所有工作簿和工作表
def read_sheet(file_path):
    dfs = []
    for file in os.listdir(file_path):
        file_data = pd.read_excel(file_path + "\\" + file)
        dfs.append(file_data)
    return pd.concat(dfs, sort=False)



# *****************************************连接mysql、sql server*****************************************#
engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '123456', 'localhost', '3306', 'erp_jd_dwd')) 
engine1 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '123456', 'localhost', '3306', 'erp_jd_dws')) 
engine2 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '123456', 'localhost', '3306', 'bi'))
engine3 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '123456', 'localhost', '3306', 'erp_jd_ads')) 

df_product          = pd.read_sql_query(text('select 物料名称 wuliaomc,`库存结余+现货可发` inventory_wl from key_product;'), engine3.connect())
df_warehouse_dayend = pd.read_sql_query(text('select wuliaomc,riqi,inventory_wl from erp_jd_dws_warehouse_dayend;'), engine1.connect())
df_sales            = pd.read_sql_query(text('select * from erp_jd_dwd_dim_saleshipping;'), engine.connect())
wuliaomc_merge      = pd.read_sql_query(text('select * from wuliaomc_merge;'), engine1.connect())

df_pro = read_sheet(path)[read_sheet(path)['类别']=='常规桌游']['名称']
df_pro = pd.merge(df_pro,wuliaomc_merge,left_on = ['名称'],right_on=['wuliaomc'],how='left')
df_holiday = pd.read_excel(r'C:\Users\liujin02\Desktop\BI建设\API_BI\本地数据源\vacation.xlsx')

df_holiday['date'] = pd.to_datetime(df_holiday['date'],format='%Y-%m-%d')

df_sales = df_sales[~df_sales['kehumc'].isin(['杭州游卡文化创意有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司'])]
listPro = df_pro['wlmc_all'].drop_duplicates().to_list()
df_sales = df_sales[df_sales['wuliaomc'].isin(listPro)]


df_warehouse_dayend['riqi'] = pd.to_datetime(df_warehouse_dayend['riqi'],format='%Y-%m-%d')









# 将存在其它字符表示分类的特征转化为数字
def dict_(df,name):

    df_wl = df[name].drop_duplicates().reset_index(drop=True)
    df_wl1 = df_wl.reset_index(drop=True).reset_index()
    df_wl1['index'] = df_wl1['index']+1 # 索引+1，后续填充0时防止和有序变量弄混
    return dict(zip(df_wl1[name],df_wl1['index'])), df_wl


# 创建日期辅助表
def create_assist_date(datestart,dateend):
    date_list = []
    date_list.append(datestart)
    while datestart < dateend:
        datestart+=timedelta(days=+1)
        date_list.append(datestart)
    return pd.DataFrame(date_list,columns=['riqi'])


def change(df2):
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















# 渠道预测
def func_qd(df_sales):
    df_sales = df_sales[df_sales['bumen']=='渠道']

    df_sales['hanshuidj_min'] = df_sales['hanshuidj'] 
    df_sales['hanshuidj_med'] = df_sales['hanshuidj'] 
    df_sales['hanshuidj_max'] = df_sales['hanshuidj'] 	
    df_sale = df_sales.groupby(['wuliaomc','riqi'],as_index = False).agg({'shifasl':'sum','hanshuidj_min':'min','hanshuidj_med':'median','hanshuidj_max':'max'})

    df = df_sale.copy()
    # 插入日期
    listFz = []
    dateMax = df['riqi'].max()
    for i in dict_(df,'wuliaomc')[1]:
        dfMid = df[df['wuliaomc']==i]
        dateMin = dfMid['riqi'].min()
        dateFz = create_assist_date(dateMin,dateMax)
        dfMid = pd.merge(dateFz,dfMid,on =['riqi'],how = 'left')
        dfMid['shifasl'].fillna(0,inplace=True)	
        dfMid.fillna(method='ffill',inplace=True)	
        listFz.append(dfMid)
    df = pd.concat(listFz,ignore_index=True)

    df = change(df)
    # 增加库存
    df = pd.merge(df,df_warehouse_dayend,on=['riqi','wuliaomc'],how='left')
    df['inventory_wl'].fillna(method='ffill',inplace=True)

    df['weeknum'] = df['riqi'].map(lambda x: int(datetime.strptime(str(x)[:10],'%Y-%m-%d').strftime('%W'))+1)
    df['month'] = df['riqi'].map(lambda x:x.month)

    df['y_m'] = df['year']*100 + df['month']
    df['y_w'] = df['year']*100 + df['weeknum']
    # 辅助日期表，计算每月假日天数
    dff = change(create_assist_date(df['riqi'].min(),df['riqi'].max()))
    dff['weeknum'] = dff['riqi'].map(lambda x: int(datetime.strptime(str(x)[:10],'%Y-%m-%d').strftime('%W'))+1)

    dff['y_m'] = dff['year']*100 + dff['month']
    dff['y_w'] = dff['year']*100 + dff['weeknum']
    dff['status_w'] = dff['status'].map(lambda x:1 if x == 0 else 0)
    dffm = dff.groupby('y_m',as_index=False).agg({'status':'sum','status_w':'sum'})
    dffw = dff.groupby('y_w',as_index=False).agg({'status':'sum','status_w':'sum'})

    # dfm = df.groupby(['wuliaomc','y_m'],as_index = False).agg({'shifasl':'sum','hanshuidj_min':'min','hanshuidj_med':'median','hanshuidj_max':'max',\
    #                                                     'year':'min','month':'min','IsVacation':'min'})
    # dfw = df.groupby(['wuliaomc','y_w'],as_index = False).agg({'shifasl':'sum','hanshuidj_min':'min','hanshuidj_med':'median','hanshuidj_max':'max',\
    #                                                     'year':'min','month':'min','IsVacation':'min','weeknum':'min'})       
    dfm = df.groupby(['wuliaomc','y_m'],as_index = False).agg({'shifasl':'sum','hanshuidj_min':'min','hanshuidj_med':'median','hanshuidj_max':'max',\
                                                        'year':'min','month':'min','IsVacation':'min','inventory_wl':'mean'})
    dfw = df.groupby(['wuliaomc','y_w'],as_index = False).agg({'shifasl':'sum','hanshuidj_min':'min','hanshuidj_med':'median','hanshuidj_max':'max',\
                                                        'year':'min','month':'min','IsVacation':'min','weeknum':'min','inventory_wl':'mean'})                      
    dfmr = pd.merge(dfm,dffm,on=['y_m'],how='left')
    dfwr = pd.merge(dfw,dffw,on=['y_w'],how='left')
    # 计算每月天数
    listDay = []
    for i in range(len(dfmr)):
        listDay.append(calendar.monthrange(dfmr['year'][i],dfmr['month'][i])[1])
    dfmr['day'] = pd.DataFrame(listDay)


    # 返回当下的年月，后面原表剔除此值方便验证和预测
    y_m = datetime.now().year*100+datetime.now().month
    y_w = dateMax.year*100 + int(dateMax.strftime('%W'))+1
    dfmr = dfmr[dfmr['y_m'] != y_m]
    dfwr = dfwr[dfwr['y_w'] != y_w]

    z = df_sales[['wuliaofzmc','wuliaomc']].drop_duplicates()
    def funcA(dfmr,name):
        
        dfmr = pd.merge(dfmr,z,on=['wuliaomc'],how='left')
        dfmr.sort_values(by=[name],inplace=True)
        dfmr.reset_index(inplace=True)
        # 分类的特征转化为数字
        dict_wl = dict_(dfmr,'wuliaomc')[0]
        dict_fz = dict_(dfmr,'wuliaofzmc')[0]
        # print(dict_fz)
        dfmr['wuliaomc'].replace(dict_wl,inplace=True)
        dfmr['wuliaofzmc'].replace(dict_fz,inplace=True)
        return dfmr


    # 赋表，选择月预测或周预测修改此处
    dfr = dfwr.copy()
    name = 'y_w'
    dfr = funcA(dfr,name)

    #删掉训练和测试数据集中不需要的特征
    df_train = dfr.drop([name,'index'],axis=1)


    # 设置数据集，切分数据，固定随机种子（random_state）时，同样的代码，得到的训练集数据相同。
    x, y = df_train.drop(['shifasl'],axis=1),df_train['shifasl']
    Xtrain, Xtest, ytrain, ytest = model_selection.train_test_split( x, y, test_size=0.3, random_state=42)

    # 调参
    cv_params = {'learning_rate': [0.01,0.02]}
    other_params = {'n_estimators':500, 'max_depth':12, 'min_child_weight': 4, 'gamma': 0.2, 
                    'subsample': 0.9, 'colsample_bytree': 0.6, 'reg_alpha': 0.5, 'reg_lambda': 3, 'learning_rate': 0.02,'seed': 10}
    
    model = xgb.XGBRegressor(**other_params)
    optimized_GBM = GridSearchCV(estimator=model, param_grid=cv_params, scoring='r2', cv=5, verbose=1)
    gbm = optimized_GBM.fit(Xtrain, ytrain)
    print('参数的最佳取值:{0}'.format(gbm.best_params_))
    print('最佳模型得分:{0}'.format(gbm.best_score_))


    #采用保留数据集进行检测
    Xtest.sort_index(inplace=True)
    ytest.sort_index(inplace=True)
    yhat = gbm.predict(Xtest)
    res=pd.DataFrame(data=ytest)
    res['Predicition']=yhat
    res=pd.merge(Xtest,res,left_index=True,right_index=True)
    res['Ratio']=res['Predicition']/res['shifasl']


    # 以数据集的未来一天开始作为预测的第一天，共预测30天
    date_start = df_sales['riqi'].max()+timedelta(1)
    date_end = df_sales['riqi'].max()+timedelta(35)
    df_t = create_assist_date(date_start,date_end)
    df_t = change(df_t)
    df_t['status_w'] = df_t['status'].map(lambda x:1 if x == 0 else 0)
    df_t['weeknum'] = df_t['riqi'].map(lambda x: int(datetime.strptime(str(x)[:10],'%Y-%m-%d').strftime('%W'))+1)
    df_t['y_w'] = df_t['year']*100 + df_t['weeknum']

    # 增加类别项
    df_t['wuliaomc'] = pd.Series() 
    list_t = []
    for i in dict_(dfmr,'wuliaomc')[1]:
        df_t1 = df_t.fillna(i)
        list_t.append(df_t1)
    df_test = pd.concat(list_t, sort=False)
    df_test = df_test.groupby(['y_w','wuliaomc'],as_index=False).agg({'year':'max','month':'max','IsVacation':'max','status':'sum','status_w':'sum','weeknum':'max'})

    # 原特征提取
    dfIndex = dfmr.reset_index()
    dfIndex1 = dfIndex.groupby('wuliaomc',as_index=False).agg({'y_m':'max','index':'max'})
    listIndex = dfIndex1['index'].to_list()
    dfIndex2 = dfIndex[dfIndex['index'].isin(listIndex)][['hanshuidj_min','hanshuidj_med','hanshuidj_max']]
    dfIndex2.reset_index(drop=True,inplace=True)
    dfConcat = pd.concat([dfIndex1,dfIndex2],axis=1)
    dfConcat.drop(['index'],axis=1,inplace= True)

    df_test = pd.merge(df_test,dfConcat,on=['wuliaomc'],how = 'left')
    # 增加现时库存
    df_test = pd.merge(df_test,df_product,on=['wuliaomc'],how='left')

    df_test['wuliaomc'].replace(dict_(dfmr,'wuliaomc')[0],inplace=True) # 特征替换
    df_test.drop(['y_w'],axis=1,inplace=True)
    z1 = dfr[['wuliaofzmc','wuliaomc']].drop_duplicates()
    df_test = pd.merge(df_test,z1,on=['wuliaomc'],how='left')
    

    df_test = df_test[x.columns] # 列标签顺序调整

    # 测试集预测
    df_test['Predicition'] = gbm.predict(df_test)
    res['wuliaomc'] = res['wuliaomc'].map(lambda x:getDictKey(dict_(dfmr,'wuliaomc')[0],x,0))
    df_test['wuliaomc'] = df_test['wuliaomc'].map(lambda x:getDictKey(dict_(dfmr,'wuliaomc')[0],x,0))

    return df_test















# 电商预测
def func_ds(df_sales):
    df_sales = df_sales[df_sales['bumen']=='电商平台部']

    df_sales['hanshuidj_min'] = df_sales['hanshuidj'] 
    df_sales['hanshuidj_med'] = df_sales['hanshuidj'] 
    df_sales['hanshuidj_max'] = df_sales['hanshuidj'] 	
    df_sale = df_sales.groupby(['wuliaomc','riqi'],as_index = False).agg({'shifasl':'sum','hanshuidj_min':'min','hanshuidj_med':'median','hanshuidj_max':'max','beizhu':'count'})
    # 部门
    a = df_sales.groupby(['wuliaomc','riqi','kehumc']).agg({'shifasl':'sum'}).unstack()
    a.columns = a.columns.droplevel()
    a.reset_index(drop=True,inplace=True)
    df = pd.concat([df_sale,a],axis=1)
    # 插入日期
    listFz = []
    dateMax = df['riqi'].max()
    for i in dict_(df,'wuliaomc')[1]:
        dfMid = df[df['wuliaomc']==i]
        dateMin = dfMid['riqi'].min()
        dateFz = create_assist_date(dateMin,dateMax)
        dfMid = pd.merge(dateFz,dfMid,on =['riqi'],how = 'left')
        dfMid['shifasl'].fillna(0,inplace=True)	
        dfMid.fillna(method='ffill',inplace=True)	
        listFz.append(dfMid)
    df = pd.concat(listFz,ignore_index=True)

    df = change(df)
    df['weeknum'] = df['riqi'].map(lambda x: int(datetime.strptime(str(x)[:10],'%Y-%m-%d').strftime('%W'))+1)
    df['month'] = df['riqi'].map(lambda x:x.month)

    df['y_m'] = df['year']*100 + df['month']
    df['y_w'] = df['year']*100 + df['weeknum']
    # 辅助日期表，计算每月假日天数
    dff = change(create_assist_date(df['riqi'].min(),df['riqi'].max()))
    dff['weeknum'] = dff['riqi'].map(lambda x: int(datetime.strptime(str(x)[:10],'%Y-%m-%d').strftime('%W'))+1)

    dff['y_m'] = dff['year']*100 + dff['month']
    dff['y_w'] = dff['year']*100 + dff['weeknum']
    dff['status_w'] = dff['status'].map(lambda x:1 if x == 0 else 0)
    dffm = dff.groupby('y_m',as_index=False).agg({'status':'sum','status_w':'sum'})
    dffw = dff.groupby('y_w',as_index=False).agg({'status':'sum','status_w':'sum'})

    list_d = list(a.columns)
    c = dict(zip(list_d,['sum']*len(list_d)))
    b = {'shifasl':'sum','hanshuidj_min':'min','hanshuidj_med':'median','hanshuidj_max':'max','beizhu':'count','year':'min','month':'min','IsVacation':'min'}
    b.update(c)
    b1 = {'shifasl':'sum','hanshuidj_min':'min','hanshuidj_med':'median','hanshuidj_max':'max','beizhu':'count','year':'min','month':'min','IsVacation':'min','weeknum':'min'}
    b1.update(c)


    dfm = df.groupby(['wuliaomc','y_m'],as_index = False).agg(b)
    dfw = df.groupby(['wuliaomc','y_w'],as_index = False).agg(b1)                       
    dfmr = pd.merge(dfm,dffm,on=['y_m'],how='left')
    dfwr = pd.merge(dfw,dffw,on=['y_w'],how='left')
    # 计算每月天数
    listDay = []
    for i in range(len(dfmr)):
        listDay.append(calendar.monthrange(dfmr['year'][i],dfmr['month'][i])[1])
    dfmr['day'] = pd.DataFrame(listDay)

    
    for i in list_d:
        dfmr[i] = dfmr[i].map(lambda x:1 if x>0 else 0)
        dfwr[i] = dfwr[i].map(lambda x:1 if x>0 else 0)

    # 返回当下的年月，后面原表剔除此值方便验证和预测
    y_m = datetime.now().year*100+datetime.now().month
    y_w = dateMax.year*100 + int(dateMax.strftime('%W'))+1
    dfmr = dfmr[dfmr['y_m'] != y_m]
    dfwr = dfwr[dfwr['y_w'] != y_w]

    z = df_sales[['wuliaofzmc','wuliaomc']].drop_duplicates()
    def funcA(dfmr,name):
        
        dfmr = pd.merge(dfmr,z,on=['wuliaomc'],how='left')
        dfmr.sort_values(by=[name],inplace=True)
        dfmr.reset_index(inplace=True)
        # 分类的特征转化为数字
        dict_wl = dict_(dfmr,'wuliaomc')[0]
        dict_fz = dict_(dfmr,'wuliaofzmc')[0]
        # print(dict_fz)
        dfmr['wuliaomc'].replace(dict_wl,inplace=True)
        dfmr['wuliaofzmc'].replace(dict_fz,inplace=True)
        return dfmr


    # 赋表，选择月预测或周预测修改此处
    dfr = dfwr.copy()
    name = 'y_w'
    dfr = funcA(dfr,name)

    #删掉训练和测试数据集中不需要的特征
    df_train = dfr.drop([name,'index'],axis=1)


    # 设置数据集，切分数据，固定随机种子（random_state）时，同样的代码，得到的训练集数据相同。
    x, y = df_train.drop(['shifasl'],axis=1),df_train['shifasl']
    Xtrain, Xtest, ytrain, ytest = model_selection.train_test_split( x, y, test_size=0.3, random_state=42)


    # 调参
    cv_params = {'learning_rate': [0.01,0.02]}
    other_params = {'n_estimators': 120, 'max_depth':13, 'min_child_weight': 4, 'gamma': 0.1, 
                    'subsample': 0.7, 'colsample_bytree': 0.6, 'reg_alpha': 3, 'reg_lambda': 3, 'learning_rate': 0.02,'seed': 10}
    
    model = xgb.XGBRegressor(**other_params)
    optimized_GBM = GridSearchCV(estimator=model, param_grid=cv_params, scoring='r2', cv=5, verbose=1)
    gbm = optimized_GBM.fit(Xtrain, ytrain)
    print('参数的最佳取值:{0}'.format(gbm.best_params_))
    print('最佳模型得分:{0}'.format(gbm.best_score_))



    #采用保留数据集进行检测
    Xtest.sort_index(inplace=True)
    ytest.sort_index(inplace=True)
    yhat = gbm.predict(Xtest)
    res=pd.DataFrame(data=ytest)
    res['Predicition']=yhat
    res=pd.merge(Xtest,res,left_index=True,right_index=True)
    res['Ratio']=res['Predicition']/res['shifasl']


    # 以数据集的未来一天开始作为预测的第一天，共预测30天
    date_start = df_sales['riqi'].max()+timedelta(1)
    date_end = df_sales['riqi'].max()+timedelta(35)
    df_t = create_assist_date(date_start,date_end)
    df_t = change(df_t)
    df_t['status_w'] = df_t['status'].map(lambda x:1 if x == 0 else 0)
    df_t['weeknum'] = df_t['riqi'].map(lambda x: int(datetime.strptime(str(x)[:10],'%Y-%m-%d').strftime('%W'))+1)
    df_t['y_w'] = df_t['year']*100 + df_t['weeknum']

    # 增加类别项
    df_t['wuliaomc'] = pd.Series() 
    list_t = []
    for i in dict_(dfmr,'wuliaomc')[1]:
        list_t.append(df_t.fillna(i))
    df_test = pd.concat(list_t, sort=False)
    df_test = df_test.groupby(['y_w','wuliaomc'],as_index=False).agg({'year':'max','month':'max','IsVacation':'max','status':'sum','status_w':'sum','weeknum':'max'})

    # 原特征提取
    c1 = dict(zip(list_d,['max']*len(list_d)))
    dfDepart = dfmr.groupby('wuliaomc',as_index=False).agg(c1)
    dfIndex = dfmr.reset_index()
    dfIndex1 = dfIndex.groupby('wuliaomc',as_index=False).agg({'y_m':'max','index':'max'})
    listIndex = dfIndex1['index'].to_list()
    dfIndex2 = dfIndex[dfIndex['index'].isin(listIndex)][['hanshuidj_min','hanshuidj_med','hanshuidj_max']]
    dfIndex2.reset_index(drop=True,inplace=True)
    dfConcat = pd.concat([dfDepart,dfIndex2],axis=1)

    df_test = pd.merge(df_test,dfConcat,on=['wuliaomc'],how = 'left')
    df_test['wuliaomc'].replace(dict_(dfmr,'wuliaomc')[0],inplace=True) # 特征替换
    df_test.drop(['y_w'],axis=1,inplace=True)
    df_test['beizhu'] = 0
    z1 = dfr[['wuliaofzmc','wuliaomc']].drop_duplicates()
    df_test = pd.merge(df_test,z1,on=['wuliaomc'],how='left')

    # 计算每月天数
    listDay1 = []
    for i in range(len(df_test)):
        listDay1.append(calendar.monthrange(df_test['year'][i],df_test['month'][i])[1])
    df_test['day'] = pd.DataFrame(listDay1)

    df_test = df_test[x.columns] # 列标签顺序调整

    # 测试集预测
    df_test['Predicition'] = gbm.predict(df_test)
    res['wuliaomc'] = res['wuliaomc'].map(lambda x:getDictKey(dict_(dfmr,'wuliaomc')[0],x,0))
    df_test['wuliaomc'] = df_test['wuliaomc'].map(lambda x:getDictKey(dict_(dfmr,'wuliaomc')[0],x,0))

    return df_test







# 预测结果修正
def func_y(df_sales,df_test,bumen):
    df_sales = df_sales[df_sales['bumen']==bumen]

    # 年度系数
    df_xs = df_sales.groupby(['month','year'])['shifasl'].sum().unstack()
    df_mxs = df_xs.div(df_xs.iloc[0:1].values, axis=1)
    df_yxs = df_xs.div(df_xs['2019'].values, axis=0)

    # 增加近1，3，6月均销量
    df180 = df_sales[df_sales['riqi'] >= datetime.now()-timedelta(210)]
    df90 = df_sales[df_sales['riqi'] >= datetime.now()-timedelta(105)]
    df30 = df_sales[df_sales['riqi'] >= datetime.now()-timedelta(35)]
    df180 = df180.groupby(['wuliaomc'],as_index = False).agg({'shifasl':'sum'})
    df90 = df90.groupby(['wuliaomc'],as_index = False).agg({'shifasl':'sum'})
    df30 = df30.groupby(['wuliaomc'],as_index = False).agg({'shifasl':'sum'})
    df90['shifasl'] = round(df90['shifasl']/3,0)
    df180['shifasl'] = round(df180['shifasl']/6,0)
    df30.rename(columns={'shifasl':'30days_mean'},inplace=True)
    df90.rename(columns={'shifasl':'90days_mean'},inplace=True)
    df180.rename(columns={'shifasl':'180days_mean'},inplace=True)


    df_test_m = df_test.groupby(['wuliaomc'],as_index=False)['Predicition'].sum()
    df_test_m = pd.merge(df_test_m,df30,on=['wuliaomc'],how='left')
    df_test_m = pd.merge(df_test_m,df90,on=['wuliaomc'],how='left')
    df_test_m = pd.merge(df_test_m,df180,on=['wuliaomc'],how='left')
    df_test_m.fillna(0,inplace=True)
    # 根据销量进行预测的条件约束
    df_test_m['Predicition'] = df_test_m['Predicition'].map(lambda x: round(x,0) if x>0 else 0)
    for i in range(len(df_test_m)):
        if df_test_m['90days_mean'][i] == 0:
            df_test_m['Predicition'][i] = 0
    for i in range(len(df_test_m)):
        if df_test_m['Predicition'][i] > 2*df_test_m['180days_mean'][i] and df_test_m['Predicition'][i] > 2*df_test_m['90days_mean'][i] and df_test_m['Predicition'][i] > 2*df_test_m['30days_mean'][i]:
            df_test_m['Predicition'][i] = round(0.75*max(df_test_m['30days_mean'][i],df_test_m['90days_mean'][i],df_test_m['180days_mean'][i]),0)
        elif df_test_m['Predicition'][i] < 0.5*df_test_m['180days_mean'][i] and df_test_m['Predicition'][i] < 0.5*df_test_m['90days_mean'][i] and df_test_m['Predicition'][i] < 0.5*df_test_m['30days_mean'][i]:
            df_test_m['Predicition'][i] = round(1.25*min(df_test_m['30days_mean'][i],df_test_m['90days_mean'][i],df_test_m['180days_mean'][i]),0)
    
    return df_test_m








df_qd = func_y(df_sales,func_qd(df_sales),'渠道')
df_ds = func_y(df_sales,func_ds(df_sales),'电商平台部')

df_qd['time'] = datetime.now()
df_ds['time'] = datetime.now()


# *****************************************写入数据库*******************************************#
df_qd.to_sql('bi_forecastsales_wlqd', engine2, schema='bi', if_exists='replace', index=False)
df_ds.to_sql('bi_forecastsales_wlds', engine2, schema='bi', if_exists='replace', index=False)


df_qd.to_sql('bi_forecastsales_wlqd_history', engine2, schema='bi', if_exists='append', index=False)
df_ds.to_sql('bi_forecastsales_wlds_history', engine2, schema='bi', if_exists='append', index=False)


engine.dispose()
engine1.dispose()
engine2.dispose()
engine3.dispose()



print("\n","END FORECAST_SALES",datetime.now(),"\n")