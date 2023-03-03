# -*- coding: utf-8 -*-
# 测试环境: python3.10.1

# *****************************************调库*************************************************#
import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\forecast_sales\code')

from forecast_code import *
import pandas as pd
import numpy as np
import seaborn as sns
import xgboost as xgb                              # xgboost              1.5.0

from sqlalchemy import create_engine,text
from datetime import datetime
from datetime import timedelta
from matplotlib import pyplot as plt
from sklearn import model_selection
                
from sklearn.model_selection import GridSearchCV   # scikit-learn              0.24.1
from sklearn.metrics import mean_squared_error
from sklearn.linear_model import BayesianRidge, LinearRegression, ElasticNet
from sklearn.svm import SVR
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import cross_val_score
from sklearn.metrics import explained_variance_score, mean_absolute_error, mean_squared_error, r2_score


# *****************************************连接取数*********************************************#
engine  = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '123456', 'localhost', '3306', 'erp_jd_dwd'))  
engine1 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '123456', 'localhost', '3306', 'bi'))
conn = create_engine("mssql+pymssql://{}:{}@{}:{}/{}".format('sa', '123456', '10.242.21.1', '1433', 'bi'))

df_sales       = pd.read_sql_query(text('select * from erp_jd_dwd_dim_saleshipping;'), engine.connect())
api_erp_client = pd.read_sql_query(text('select * from erp_jd_dwd_fact_client;'),      engine.connect())
df_holiday     = pd.read_sql_query(text('select * from bi_vacation;'),                engine1.connect())

df_holiday['date'] = pd.to_datetime(df_holiday['date'],format='%Y-%m-%d')

df_pro = pd.read_excel(r'C:\Users\liujin02\Desktop\BI建设\API_BI\forecast_sales\产品名称.xlsx')
listPro = df_pro['产品名称'].to_list()
df_sales = df_sales[~df_sales['wuliaomc'].isin(listPro)]


# *****************************************特征清洗*********************************************#
# 数据预处理
df_sales['weeknum'] = df_sales['riqi'].map(lambda x: int(datetime.strptime(str(x)[:10],'%Y-%m-%d').strftime('%W'))+1)
df = df_sales.groupby(['riqi','wuliaofzmc']).agg({'shifasl':'sum','jiashuihj':'sum','kehuid':'count'})
df.reset_index(inplace=True)

df1 = df_sales.groupby(['year','weeknum','wuliaofzmc']).agg({'wuliaomc':'unique'})
df1.reset_index(inplace=True)

df_qd = datena(df,'wuliaofzmc',df_holiday)[0]

df_qd['status_vaca'] = df_qd['status'].map(lambda x:1 if x == 1 else 0)
df_qd['status_work'] = df_qd['status'].map(lambda x:1 if x == 2 else 0)
df_qd['weeknum'] = df_qd['riqi'].map(lambda x: int(datetime.strptime(str(x)[:10],'%Y-%m-%d').strftime('%W'))+1)
df_qd['vacation'] = df_qd['weeknum'].map(lambda x:1 if x in [1,2,3,4,5,6,7,8,27,28,29,30,31,32,33,34,35] else 0)# 1为寒暑假，0为非寒暑假

df1['wuliaomc'] = df1['wuliaomc'].map(lambda x:len(x))
# df1['name_prov1'] = df1['name_prov1'].map(lambda x:len(x))
df1['year'] = df1['year'].astype(int)

table = df_qd.groupby(['wuliaofzmc','year','weeknum']).agg({'shifasl':'sum','jiashuihj':'sum','kehuid':'sum','status_work':'sum','status_vaca':'sum'})#
table.reset_index(inplace=True)
table['price'] = table['jiashuihj']/table['shifasl']
table = table.drop(columns=['jiashuihj'],axis=1).reset_index() 

df_qd = pd.merge(table,df1 ,on = ['wuliaofzmc','year','weeknum'],how='left')
df_qd.fillna(0,inplace=True)

# 分类的特征转化为数字
dict_wl = dict_(df_qd,'wuliaofzmc')[0]
print(dict_wl)
df_qd['wuliaofzmc'].replace(dict_wl,inplace=True)


# *****************************************后续处理*********************************************#
df_qd['yweek'] = df_qd['year']*100+df_qd['weeknum']
df_qd.sort_values(['yweek','wuliaofzmc'],inplace=True) # 时间升序
df_qd.reset_index(drop=True,inplace=True)
df_qd['shifasl'] = df_qd['shifasl'].replace(0,1) # 由于后续涉及的计算，销量用1填充
df_qd = df_qd.drop(['index'],axis=1).reset_index()
#删掉训练和测试数据集中不需要的特征
df_train = df_qd.drop(['index','yweek'],axis=1)

# 分析训练数据集中特征相关性
# 保留特征
# ['year','weeknum','wuliaofzmc','kehuid'（客户数量）,'status_work'（工作日）,'status_vaca'（假期）,
# 'price','wuliaomc'（sku数量）,'name_prov1'（省份数量）,'vacation'（寒暑假）]
plt.subplots(figsize=(24,20))
sns.heatmap(df_train.corr(),cmap='RdYlGn',annot=True,vmin=-0.1,vmax=0.1,center=0)


# ***************************************设置数据集*********************************************#
# 设置数据集，切分数据，固定随机种子（random_state）时，同样的代码，得到的训练集数据相同。
x, y = df_train.drop(['shifasl'],axis=1),df_train['shifasl']
Xtrain, Xtest, ytrain, ytest = model_selection.train_test_split( x, y, test_size=0.1, random_state=42)

# 查看分布
pytrain = ytrain.reset_index(drop=True)
plt.figure(figsize=(26,4))   
plt.plot(pytrain.index,pytrain.values)

pytest = ytest.reset_index(drop=True)
plt.figure(figsize=(26,4))   
plt.plot(pytest.index,pytest.values)


# *************************************定义测试集***********************************************#
# 以数据集的未来一天开始作为预测的第一天，共预测6周
date_start = df_sales['riqi'].max()+timedelta(1)
date_end = df_sales['riqi'].max()+timedelta(30)

# 生成测试集
df_t = create_assist_date(date_start,date_end)
df_t = change(df_t,df_holiday)
df_t['wuliaofzmc'] = pd.Series() 

# 增加类别项
list_t = []
for i in dict_(df_qd,'wuliaofzmc')[1]:
    df_t1 = df_t.fillna(i)
    list_t.append(df_t1)
df_test = pd.concat(list_t, sort=False)

df_test['wuliaofzmc'].replace(dict_wl,inplace=True) # 特征替换
df_test['weeknum'] = df_test['riqi'].map(lambda x: int(datetime.strptime(str(x)[:10],'%Y-%m-%d').strftime('%W'))+1)
df_test['vacation'] = df_test['weeknum'].map(lambda x:1 if x in [1,2,3,4,5,6,7,8,27,28,29,30,31,32,33,34,35] else 0)# 1为寒暑假，0为非寒暑假
df_test['status_vaca'] = df_test['status'].map(lambda x:1 if x == 1 else 0)
df_test['status_work'] = df_test['status'].map(lambda x:1 if x == 2 else 0)

table1 = df_test.groupby(['wuliaofzmc','year','weeknum']).agg({'status_work':'sum','status_vaca':'sum','vacation':'max',})# 'status_vaca':'sum',
table1.reset_index(inplace=True)

table1['yweek'] = table1['year']*100+table1['weeknum']
table1.sort_values(['yweek','wuliaofzmc'],inplace=True) # 时间升序
table1.reset_index(drop=True,inplace=True)

# 该模块采用不同时间段特征，两者可更换
# -------------------------------------------------------------------------------------------------------------------------------
# table1['year1'] = table1['year']-1
# df_qd1 = df_qd[['year','weeknum','wuliaofzmc','kehuid','price','wuliaomc','name_prov1']]
# df_test = pd.merge(table1,df_qd1,left_on=['wuliaofzmc','year1','weeknum'],right_on=['wuliaofzmc','year','weeknum'],how='left')
# #删掉训练和测试数据集中不需要的特征
# df_test = df_test.drop(['yweek','year_y','year1'],axis=1)
# df_test.rename(columns={'year_x':'year'},inplace=True)
# -------------------------------------------------------------------------------------------------------------------------------
table2 = df_qd.iloc[-len(table1):][['price','kehuid','wuliaomc']]
table2.reset_index(drop=True,inplace=True)
df_test = pd.concat([table1 ,table2 ],axis=1)
df_test = df_test.drop(['yweek'],axis=1)
# -------------------------------------------------------------------------------------------------------------------------------
df_test = df_test[Xtrain.columns] # 列标签顺序调整


# ***************************************模型评估***********************************************#
model_br = BayesianRidge()  # 建立贝叶斯回归模型
model_lr = LinearRegression() # 建立普通线性回归模型
model_etc = ElasticNet() # 建立弹性网络回归模型
model_svr = SVR() # 建立支持向量回归模型
model_gbr = GradientBoostingRegressor() # 建立梯度增强回归模型
model_xgb = xgb.XGBRegressor(objective ='reg:squarederror') # XGBoost回归模型

model_names = ['BayesianRidge','LinearRegression','ElasticNet', 'SVR', 'GBR','XGBR']
model_dir = [model_br, model_lr, model_etc, model_svr, model_gbr, model_xgb]

# 交叉验证评分与模型训练
cv_score_list = [] # 交叉检验结果列表
y_train_pre = [] # 各个模型预测的y值列表
y_test_pre = [] # 创建测试集预测结果列表
n_folds = 20 # 设置交叉检验的次数

for model in model_dir:
    scores = cross_val_score(model, Xtrain, ytrain, cv = n_folds,scoring = 'r2')
    #对每个回归模型进行交叉验证,返回每次模型得分
    cv_score_list.append(scores) # 将验证结果保存在列表中
    y_train_pre.append(model.fit(Xtrain, ytrain).predict(Xtrain))
    y_test_pre.append(model.fit(Xtrain, ytrain).predict(Xtest))
    #将训练模型的预测结果保存在列表中
print(cv_score_list)
 
# 模型效果评估
n_samples, n_features = Xtrain.shape # 总训练样本量，总特征量
n_samples_test = Xtest.shape[0]
print("总训练样本:{}，总特征量:{}" .format(n_samples,n_features) )
print("总测试样本:{}" .format(n_samples_test) )

# EV: 解释回归模型的方差得分，[0,1]，接近1说明自变量越能解释因变量的方差变化
# MAE: 平均绝对误差，评估预测结果和真实数据集的接近程度的程度，越小越好
# MSE: 均方差，计算拟合数据和原始数据对应样本点的误差的平方和的均值，越小越好
# R2: 判定系数，解释回归模型的方差得分，[0,1]，接近1说明自变量越能解释因变量的方差变化。
model_metrics_name =[explained_variance_score, mean_absolute_error, mean_squared_error,r2_score]
model_metrics_list = [] # 回归评价指标列表

for i in range(len(model_dir)):
    x = [] 
    for one in model_metrics_name:  
        tmp_score = one(ytest,y_test_pre[i])  
        x.append(tmp_score) 
    model_metrics_list.append(x)  

model_metrics = pd.DataFrame(model_metrics_list,columns=['explained_variance_score','mean_absolute_error', 'mean_squared_error','r2_score'],index=model_names)
print(model_metrics)


# ***************************************超参调优***********************************************#
# loss：损失函数，ls（Least squares），默认方法，是基于最小二乘法方法的基本方法，也是普通线性回归的基本方法；
# lad（Least absolute deviation）是用于回归的鲁棒损失函数，它可以降低异常值和数据噪音对回归模型的影响；
# huber是一个结合ls和lad的损失函数，它使用alpha来控制对异常值的灵敏度；
# quantile是分位数回归的损失函数，使用alpha来指定分位数用于预测间隔。
# min_samples_leaf：作为叶子节点的最小样本数。如果设置为数字，那么将指定对应数量的样本，如果设置为浮点数，则指定为总样本量的百分比。
# alpha：用于huber或quantile的调节参数。
parameters = {'loss': ['ls','lad','huber','quantile'],
               'min_samples_leaf': [5,6,7,8],
               'alpha': [0.1,0.3,0.6,0.9]}# 定义要优化的参数信息
model_gs = GridSearchCV(estimator=model_gbr, param_grid=parameters, cv=20)# 建立交叉检验模型对象
model_gs = model_gs.fit(Xtrain, ytrain)# 训练交叉检验模型
print('Best score is:', model_gs.best_score_)# 获得交叉检验模型得出的最优得分
print('Best parameter is:', model_gs.best_params_)# 获得交叉检验模型得出的最优参数
model_best = model_gs.best_estimator_  # 获得交叉检验模型得出的最优模型对象
model_best.fit(Xtrain, ytrain)  # 训练最优模型
model_best.fit(Xtest, ytest)  # 训练最优模型


# ***************************************验证结果***********************************************#
#构建保留数据集预测结果
Xtest.sort_index(inplace=True)
ytest.sort_index(inplace=True)
res=pd.DataFrame(data=ytest)
res['Predicition'] = model_best.predict(Xtest)
res=pd.merge(Xtest,res,left_index=True,right_index=True)
res['Ratio']=res['Predicition']/res['shifasl']


# ***************************************预测结果***********************************************#
df_test['Predicition'] = model_best.predict(df_test)
res['wuliaofzmc'] = res['wuliaofzmc'].map(lambda x:getDictKey(dict_wl,x,0))
df_test['wuliaofzmc'] = df_test['wuliaofzmc'].map(lambda x:getDictKey(dict_wl,x,0))


# *****************************************写入数据库*******************************************#
df_test.to_sql('bi_forecastsales_fz', engine1, schema='bi', if_exists='replace', index=False)
engine.dispose()
engine1.dispose()

df_test.to_sql(name='bi_forecastsales_fz', con=conn, if_exists='replace', index=False)
conn.dispose() 

res.to_excel(r'C:\Users\liujin02\Desktop\BI建设\API_BI\forecast_sales\result\val_salesfz.xlsx')
df_test.to_excel(r'C:\Users\liujin02\Desktop\BI建设\API_BI\forecast_sales\result\forecast_salesfz.xlsx')