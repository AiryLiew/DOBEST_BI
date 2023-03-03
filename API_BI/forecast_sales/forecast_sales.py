# -*- coding: utf-8 -*-
# 测试环境: python3.10.1

# *****************************************调库*************************************************#
import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\forecast_sales\code')

from forecast_code import *
import pandas as pd  
import seaborn as sns
import xgboost as xgb                             # xgboost              1.5.1
import calendar                                   # numpy                1.21.3
from sqlalchemy import create_engine,text
from datetime import datetime                     
from datetime import timedelta
from sklearn import model_selection
from matplotlib import pyplot as plt
from sklearn.model_selection import GridSearchCV  # scikit-learn              1.0.2


# *****************************************连接取数*********************************************#
engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '123456', 'localhost', '3306', 'erp_jd_dwd')) 
engine1 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '123456', 'localhost', '3306', 'erp_jd_dws')) 
engine2 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '123456', 'localhost', '3306', 'bi'))
conn = create_engine("mssql+pymssql://{}:{}@{}:{}/{}".format('sa', '123456', '10.242.21.1', '1433', 'bi'))

df_sales      = pd.read_sql_query(text('select * from erp_jd_dwd_dim_saleshipping;'), engine.connect())
df_launchtime = pd.read_sql_query(text('select * from erp_jd_dws_launchtime;'),      engine1.connect())
df_holiday    = pd.read_sql_query(text('select * from bi_vacation;'),                engine2.connect())

df_holiday['date'] = pd.to_datetime(df_holiday['date'],format='%Y-%m-%d')

df_pro = pd.read_excel(r'C:\Users\liujin02\Desktop\BI建设\API_BI\forecast_sales\产品名称.xlsx')
listPro = df_pro['产品名称'].to_list()
df_sales = df_sales[~df_sales['wuliaomc'].isin(listPro)]


# *****************************************特征清洗*********************************************#
# 价格特征
df_sales['hanshuidj_min'] = df_sales['hanshuidj'] 
df_sales['hanshuidj_med'] = df_sales['hanshuidj'] 
df_sales['hanshuidj_max'] = df_sales['hanshuidj'] 
df_sale = df_sales.groupby(['wuliaomc','riqi'],as_index = False)\
                  .agg({'shifasl':'sum','hanshuidj_min':'min','hanshuidj_med':'median','hanshuidj_max':'max'})

# 部门特征
a = df_sales.groupby(['wuliaomc','riqi','bumen']).agg({'shifasl':'sum'}).unstack()
a.columns = a.columns.droplevel()
a = a[['新媒体运营部','电商平台部','渠道']].reset_index(drop=True)
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

df = change(df,df_holiday)

# 周期聚合
df['weeknum'] = df['riqi'].map(lambda x: int(datetime.strptime(str(x)[:10],'%Y-%m-%d').strftime('%W'))+1)
df['month'] = df['riqi'].map(lambda x:x.month)
df['y_m'] = df['year']*100 + df['month']
df['y_w'] = df['year']*100 + df['weeknum']

# 辅助日期表，计算每月假日天数
dff = change(create_assist_date(df['riqi'].min(),df['riqi'].max()),df_holiday)
dff['weeknum'] = dff['riqi'].map(lambda x: int(datetime.strptime(str(x)[:10],'%Y-%m-%d').strftime('%W'))+1)
dff['y_m'] = dff['year']*100 + dff['month']
dff['y_w'] = dff['year']*100 + dff['weeknum']
dff['status_w'] = dff['status'].map(lambda x:1 if x == 0 else 0)
dffm = dff.groupby('y_m',as_index=False).agg({'status':'sum','status_w':'sum'})
dffw = dff.groupby('y_w',as_index=False).agg({'status':'sum','status_w':'sum'})

# 聚合
dfm = df.groupby(['wuliaomc','y_m'],as_index = False).agg({'shifasl':'sum','hanshuidj_min':'min','hanshuidj_med':'median','hanshuidj_max':'max',\
                                                    '新媒体运营部':'sum','电商平台部':'sum','渠道':'sum','year':'min','month':'min','IsVacation':'min'})
dfw = df.groupby(['wuliaomc','y_w'],as_index = False).agg({'shifasl':'sum','hanshuidj_min':'min','hanshuidj_med':'median','hanshuidj_max':'max',\
                                                    '新媒体运营部':'sum','电商平台部':'sum','渠道':'sum','year':'min','month':'min','IsVacation':'min','weeknum':'min'})                       
dfmr = pd.merge(dfm,dffm,on=['y_m'],how='left')
dfwr = pd.merge(dfw,dffw,on=['y_w'],how='left')

# 计算每月天数
listDay = []
for i in range(len(dfmr)):
    listDay.append(calendar.monthrange(dfmr['year'][i],dfmr['month'][i])[1])
dfmr['day'] = pd.DataFrame(listDay)

# 01转换
list_d = ['新媒体运营部','电商平台部','渠道']
for i in list_d:
    dfmr[i] = dfmr[i].map(lambda x:1 if x>0 else 0)
    dfwr[i] = dfwr[i].map(lambda x:1 if x>0 else 0)

# 返回当下的年月，后面原表剔除此值方便验证和预测
y_m = datetime.now().year*100+datetime.now().month
y_w = dateMax.year*100 + int(dateMax.strftime('%W'))+1
dfmr = dfmr[dfmr['y_m'] != y_m]
dfwr = dfwr[dfwr['y_w'] != y_w]

# 赋表
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


# *****************************************赋表************************************************#
dfr = dfwr
name = 'y_w'
dfr = funcA(dfr,'y_w')
#删掉训练和测试数据集中不需要的特征
df_train = dfr.drop([name,'index'],axis=1)


# ***************************************设置数据集*********************************************#
# 切分数据，固定随机种子（random_state）时，同样的代码，得到的训练集数据相同。
x, y = df_train.drop(['shifasl'],axis=1),df_train['shifasl']
Xtrain, Xtest, ytrain, ytest = model_selection.train_test_split( x, y, test_size=0.1, random_state=42)


# ***************************************超参调优***********************************************#
cv_params = {'learning_rate':[0.02,0.03]}
other_params = {'n_estimators': 600, 'max_depth':12, 'min_child_weight': 4, 'gamma': 0.2, 
                'subsample': 0.9, 'colsample_bytree': 0.6, 'reg_alpha': 0.5, 'reg_lambda': 3, 'learning_rate': 0.02,'seed': 10}
 
model = xgb.XGBRegressor(**other_params)
optimized_GBM = GridSearchCV(estimator=model, param_grid=cv_params, scoring='r2', cv=5, verbose=1)
gbm = optimized_GBM.fit(Xtrain, ytrain)
print('参数的最佳取值:{0}'.format(gbm.best_params_))
print('最佳模型得分:{0}'.format(gbm.best_score_))


# ***************************************验证结果***********************************************#
#构建保留数据集预测结果
Xtest.sort_index(inplace=True)
ytest.sort_index(inplace=True)
yhat = gbm.predict(Xtest)
res=pd.DataFrame(data=ytest)
res['Predicition']=yhat
res=pd.merge(Xtest,res,left_index=True,right_index=True)
res['Ratio']=res['Predicition']/res['shifasl']


# *************************************定义测试集***********************************************#
# 以数据集的未来一天开始作为预测的第一天，共预测30天
date_start = df_sales['riqi'].max()+timedelta(1)
date_end = df_sales['riqi'].max()+timedelta(30)
df_t = create_assist_date(date_start,date_end)
df_t = change(df_t,df_holiday)
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
dfDepart = dfmr.groupby('wuliaomc',as_index=False).agg({'新媒体运营部':'max','电商平台部':'max','渠道':'max'})
dfIndex = dfmr.reset_index()
dfIndex1 = dfIndex.groupby('wuliaomc',as_index=False).agg({'y_m':'max','index':'max'})
listIndex = dfIndex1['index'].to_list()
dfIndex2 = dfIndex[dfIndex['index'].isin(listIndex)][['hanshuidj_min','hanshuidj_med','hanshuidj_max']]
dfIndex2.reset_index(drop=True,inplace=True)
dfConcat = pd.concat([dfDepart,dfIndex2],axis=1)

df_test = pd.merge(df_test,dfConcat,on=['wuliaomc'],how = 'left')
df_test['wuliaomc'].replace(dict_(dfmr,'wuliaomc')[0],inplace=True) # 特征替换
df_test.drop(['y_w'],axis=1,inplace=True)

z1 = dfr[['wuliaofzmc','wuliaomc']].drop_duplicates()
df_test = pd.merge(df_test,z1,on=['wuliaomc'],how='left')
# 列标签顺序调整
df_test = df_test[x.columns] 


# ***************************************预测结果***********************************************#
# 测试集预测
df_test['Predicition'] = gbm.predict(df_test)
# 名称转换
res['wuliaomc'] = res['wuliaomc'].map(lambda x:getDictKey(dict_(dfmr,'wuliaomc')[0],x,0))
df_test['wuliaomc'] = df_test['wuliaomc'].map(lambda x:getDictKey(dict_(dfmr,'wuliaomc')[0],x,0))


# *****************************************写入数据库*******************************************#
df_test.to_sql('bi_forecastsales_wl', engine2, schema='bi', if_exists='replace', index=False)
engine.dispose()
engine1.dispose()
engine2.dispose()

df_test.to_sql(name='bi_forecastsales_wl', con=conn, if_exists='replace', index=False)
conn.dispose() 

res.to_excel(r'C:\Users\liujin02\Desktop\BI建设\API_BI\forecast_sales\result\val_sales.xlsx')
df_test.to_excel(r'C:\Users\liujin02\Desktop\BI建设\API_BI\forecast_sales\result\forecast_sales.xlsx')