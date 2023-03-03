# -*- coding: utf-8 -*-
# 测试环境: python3.10.1

# *****************************************调库*************************************************#
import pandas as pd

from sqlalchemy import create_engine
from datetime import datetime
from matplotlib import pyplot as plt
# 设置显示中文字体
# 显示汉字
plt.rc("font",family='FangSong')


# *****************************************连接取数*********************************************#
engine  = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '123456', 'localhost', '3306', 'erp_jd_dwd'))  
engine1 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '123456', 'localhost', '3306', 'bi'))
conn = create_engine("mssql+pymssql://{}:{}@{}:{}/{}".format('sa', '123456', '10.242.21.1', '1433', 'bi'))

df_sales = pd.read_sql_query('select * from erp_jd_dwd_dim_saleorders;', engine)

df_sales = df_sales[(df_sales['bumen'] == '渠道')&(df_sales['danjulxmc'] != '年返销售订单')]
# 去除内部订单
list_ = ['杭州游卡文化创意有限公司','上海飞之火电竞信息科技有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司','杭州迷思文化创意有限公司-西西弗']
df_sales = df_sales[~df_sales['kehumc'].isin(list_)]
data = df_sales[['kehumc','riqi','xiaoshousl','jiashuihj']]


data.reset_index(drop=True,inplace=True)
data.reset_index(inplace=True)
column_name = 'xiaoshousl'
column_name1 = '总销量'


# 按【订单号】和【用户 ID】分组后，获取【发货日期】列的最大值和【总金额】列的总和
grouped_data = data.groupby(['index','kehumc'], as_index=False).agg({'riqi': 'max', column_name: 'sum'})
# 计算时间间隔  
today = datetime.now()
grouped_data['时间间隔'] = (pd.to_datetime(today) - pd.to_datetime(grouped_data['riqi'])).dt.days


# 按【用户 ID】分组后，获取【时间间隔】列的最小值、【订单号】列的数量，以及【总金额】列的总和
rfm_data = grouped_data.groupby('kehumc', as_index=False).agg({'时间间隔': 'min', 'index': 'count', column_name: 'sum'})
# 修改列名
rfm_data.columns = ['客户', '时间间隔', '总次数', column_name1]


# 定义函数按照区间划分 R 值
def caculate_r(s):
    if s <= 30:
        return 5
    elif s <= 60:
        return 4
    elif s <= 180:
        return 3
    elif s <= 365:
        return 2
    else:
        return 1


# 对 R 值进行评分
rfm_data['R评分'] = rfm_data['时间间隔'].agg(caculate_r)

# 定义函数按照区间划分 F 值
def caculate_f(s):
    if s <= 90:
        return 1
    elif s <= 260:
        return 2
    elif s <= 430:
        return 3
    elif s <= 600:
        return 4
    else:
        return 5

# 对 F 值进行评分
rfm_data['F评分'] = rfm_data['总次数'].agg(caculate_f)


# 定义函数按照区间划分 M 值
def caculate_m(s):
    if s <= 2000:
        return 1
    elif s <= 5000:
        return 2
    elif s <= 8000:
        return 3
    elif s <= 15000:
        return 4
    else:
        return 5


# 对 M 值进行评分
rfm_data['M评分'] = rfm_data[column_name1].agg(caculate_m)
# 计算 R评分、F评分、M评分的平均数
r_avg = rfm_data['R评分'].mean()
f_avg = rfm_data['F评分'].mean()
m_avg = rfm_data['M评分'].mean()


# 将R评分、F评分、M评分 的数据分别与对应的平均数做比较
rfm_data['R向量化'] = (rfm_data['R评分'] > r_avg) * 1
rfm_data['F向量化'] = (rfm_data['F评分'] > f_avg) * 1
rfm_data['M向量化'] = (rfm_data['M评分'] > m_avg) * 1
# 拼接R评分、F评分、M评分
rfm_score = rfm_data['R向量化'].astype(str) + rfm_data['F向量化'].astype(str) + rfm_data['M向量化'].astype(str)


# 定义字典标记 RFM 评分档对应的用户分类名称
transform_label = {
    '111':'重要价值用户',
    '101':'重要发展用户',
    '011':'重要保持用户',
    '001':'重要挽留用户',
    '110':'一般价值用户',
    '100':'一般发展用户',
    '010':'一般保持用户',
    '000':'一般挽留用户'
}

level_label = {
    '重要价值用户':'A',
    '重要发展用户':'A',
    '重要保持用户':'B',
    '重要挽留用户':'B',
    '一般价值用户':'B',
    '一般发展用户':'B',
    '一般保持用户':'C',
    '一般挽留用户':'C'
}

# 将RFM评分替换成具体的客户类型
rfm_data['客户类型'] = rfm_score.replace(transform_label)
rfm_data['客户分级'] = rfm_data['客户类型'].replace(level_label)
# 按【客户类型】分组，统计用户的数量
customer_data = rfm_data.groupby('客户类型')['客户'].count()


# *****************************************写入数据库*******************************************#
rfm_data.to_sql('bi_rfm', engine1, schema='bi', if_exists='replace', index=False)
engine.dispose()
engine1.dispose()

rfm_data.to_sql(name='bi_rfm', con=conn, if_exists='replace', index=False)
conn.dispose() 