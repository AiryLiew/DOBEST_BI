# -*- coding: utf-8 -*-
# 测试环境: python3.10.1


import pandas as pd
import math
import numpy as np
from sqlalchemy import create_engine,text
from datetime import datetime 

print("\n","START qd_dlzy", datetime.now(),"\n")


# *****************************************连接mysql、sql server*****************************************#
engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format('root', '123456', 'localhost', '3306'))


dfmm                   = pd.read_sql_query(text("select * from erp_jd_dwd.erp_jd_dwd_dim_purchaseorders where company = '杭州游卡文化创意有限公司' and wuliaomc like 'JAKKS%%' AND riqi>'2022-09-01';"),
                                            engine.connect())
df_closebalance        = pd.read_sql_query(text("select * from erp_jd_dws.erp_jd_dws_closebalance where 科目编码 = '1122.06' ;"),
                                            engine.connect())
df_saleshipping_yk     = pd.read_sql_query(text("select * from erp_jd_dwd.erp_jd_dwd_dim_saleshipping where company ='杭州游卡文化创意有限公司' and cangkumc = '客户仓' and kehumc not in ('杭州迷思文化创意有限公司','杭州游卡文化创意有限公司');"),
                                            engine.connect())
df_saleshipping_ms     = pd.read_sql_query(text("select * from erp_jd_dwd.erp_jd_dwd_dim_saleshipping where company ='杭州迷思文化创意有限公司' and kehumc not in ('杭州迷思文化创意有限公司','杭州游卡文化创意有限公司');"),
                                            engine.connect())
df_salereturn          = pd.read_sql_query(text("select * from erp_jd_dwd.erp_jd_dwd_dim_salereturn where company in ('杭州迷思文化创意有限公司','杭州游卡文化创意有限公司');"),
                                            engine.connect())
df_allocation          = pd.read_sql_query(text("select * from erp_jd_dwd.erp_jd_dwd_dim_allocation where company in ('杭州迷思文化创意有限公司','杭州游卡文化创意有限公司') and diaochuck in ('寄售仓-迷思','客户仓');"),
                                            engine.connect())
df_purchaseorders      = pd.read_sql_query(text("select * from erp_jd_dwd.erp_jd_dwd_dim_purchaseorders where company in ('杭州迷思文化创意有限公司','杭州游卡文化创意有限公司');"),
                                            engine.connect())
df_saleorders          = pd.read_sql_query(text("select * from erp_jd_dwd.erp_jd_dwd_dim_saleorders where kehumc not in ('杭州迷思文化创意有限公司','杭州游卡文化创意有限公司') and kehumc = any(select DISTINCT kehumc from erp_jd_dwd.erp_jd_dwd_dim_saleorders where danjulxmc = '寄售销售订单') and danjulxmc<>'标准销售订单';"),
                                            engine.connect())
df_saleorders_xxf_2018 = pd.read_sql_query(text("select * from localdata.saleorders_xxf_2018;"),
                                            engine.connect())
df_begin_receivable    = pd.read_sql_query(text("select 业务日期 riqi, CONVERT(year(业务日期),CHAR) `year`, LPAD(CONVERT(month(业务日期),CHAR),2,0) `month`, 客户 kehumc,含税单价 hanshuidj,价税合计 jiashuihj,物料名称 wuliaomc,计价数量 shifasl,结算组织 company from localdata.qd_begin_receivable;"),
                                            engine.connect())
list_goods             = pd.read_sql_query(text("SELECT DISTINCT wuliaomc FROM `erp_jd_ods`.`erp_jd_ods_dim_purchaseorders_ms_cwzx`  WHERE `gongyingsmc` = '杭州游卡文化创意有限公司' and wuliaomc not like 'JAKKS%%' and wuliaomc not like '万代%%';"),
                                            engine.connect())
df_pricelist           = pd.read_sql_query(text("select * from localdata.price_list_dl;"),
                                            engine.connect())
df_cost                = pd.read_sql_query(text("SELECT * FROM erp_jd_dwd.erp_jd_dwd_dim_cost;"),
                                            engine.connect())

df_t  = pd.read_excel(r'C:\Users\liujin02\Desktop\邮件报表\2019前代理数据.xlsx',sheet_name='Sheet1')
df_t1 = pd.read_excel(r'C:\Users\liujin02\Desktop\邮件报表\2019前代理数据.xlsx',sheet_name='Sheet2')

df_begin_receivable = pd.merge(df_begin_receivable,df_cost,on=['wuliaomc'],how='left')
df_begin_receivable['purchases'] = df_begin_receivable['shifasl']*df_begin_receivable['cost']
df_begin_receivable['profit'] = df_begin_receivable['jiashuihj']-df_begin_receivable['purchases']

df_saleshipping = pd.concat([df_saleshipping_yk,df_saleshipping_ms,df_begin_receivable],ignore_index=True)
df_allocation['riqi'] = pd.to_datetime(df_allocation['riqi'],format='%Y-%m-%d')



# *********************************************************************************************************************************
# 自定义年月表
def func(a,b):
    df_empty = pd.DataFrame()
    list_a = []
    list_b = []
    for i in range(a,b+1):
        for l in range(1,13):
            list_a.append(i)
            list_b.append(l)
    df_empty['year'] = pd.DataFrame(list_a)
    df_empty['month'] = pd.DataFrame(list_b)
    return df_empty


# 平均成本
df_purchasecost = df_purchaseorders.groupby(['wlmc_all'],as_index=False).agg({'caigousl_new':'sum','jiashuihj':'sum'})
df_purchasecost['cost'] = df_purchasecost['jiashuihj']/df_purchasecost['caigousl_new']




# 代理产品
# *********************************************************************************************************************************
# 采购
df_purchaseorders_ms = df_purchaseorders[(df_purchaseorders['company'] == '杭州迷思文化创意有限公司')&(df_purchaseorders['riqi'] >= datetime(2022,1,1))]
df_purchaseorders_ms = df_purchaseorders[df_purchaseorders['wlmc_all'].isin(df_t1['产品名称'].to_list()+df_purchaseorders_ms['wlmc_all'].to_list()+dfmm['wlmc_all'].to_list())]
df_purchaseorders_ms = pd.concat([df_purchaseorders_ms,dfmm,df_t[['riqi','采购金额','hanshuidj_new','caigousl_new','wuliaomc','year','month']].rename(columns={'采购金额':'jiashuihj','wuliaomc':'wlmc_all'})])

# 增加上市时间
df_purchaseorders_ms['上市月份'] = datetime.now() - df_purchaseorders_ms['riqi']
df_purchaseorders_ms.loc[:,'上市月份'] = df_purchaseorders_ms['上市月份'].map(lambda x:math.ceil(int(str(x).split(' ')[0])/30) if str(x)!='NaT' else np.nan)
df_dj = df_pricelist[['品名','商品定价']].drop_duplicates()
df_dj.rename(columns={'品名':'wlmc_all','商品定价':'零售价'},inplace=True)
df_purchaseorders_ms = pd.merge(df_purchaseorders_ms,df_dj,on=['wlmc_all'],how = 'left')


# 增加品牌
dict_pp = { '杰克士':['JAKKS'],
            '万代':['万代','游器物'],
            '小猪班克':['小猪班克'],
            '天天富翁':['天天富翁'],
            '乐童宝贝':['乐童'],
            'spinmaster':['汪汪队立大功闪闪拼图24片','汪汪队立大功大电影-木盒拼图','汪汪队立大功空中救援 - 泡沫板拼图','汪汪队立大功大电影 - 泡沫板拼图','Calm 七天拼图套装 （2700片）','Calm 名胜拼图（组合装）','流沙勇士桌游']
            }
def getdictkey(dict_,word):
    return [k for k,v in dict_.items() if any(v1 for v1 in v if v1 in word)][0]


df_purchaseorders_ms['品牌'] = df_purchaseorders_ms['wlmc_all'].map(lambda x :getdictkey(dict_pp,x))



# 销售订单
df_saleorders_dl0 = df_saleorders[(df_saleorders['wuliaomc'].isin(df_t1['产品名称'].to_list()+df_purchaseorders_ms['wlmc_all'].to_list()))]
df_saleorders_dl0 = pd.concat([df_saleorders_dl0,df_saleorders_xxf_2018[df_saleorders_xxf_2018['wuliaomc']=='小小王国-天天富翁超大豪华版'],df_t[~df_t['wuliaomc'].isin(['小小王国-天天富翁超大豪华版','小猪班克儿童饮水习惯培养杯'])][['riqi','xiaoshousl','hanshuidj','发货金额','wuliaomc','year','month','kehumc','company']].rename(columns={'发货金额':'jiashuihj'})])


# 寄售客户列表
list_kh = df_saleorders_dl0[~df_saleorders_dl0['wuliaomc'].isin(['小小王国-天天富翁超大豪华版','小猪班克儿童饮水习惯培养杯'])]['kehumc'].drop_duplicates().to_list()
# list_kh = ['四川言几又供应链管理有限公司',
# '拉加代尔旅行零售（上海）有限公司',
# '上海思鸣贸易商行',
# '大连半人马商贸有限公司',
# '台州市亮仔宝贝孕婴童用品有限公司',
# '北京天和益兴商贸有限公司',
# '湖南三知图书有限公司华创店',
# '渝北区珠心算教育信息咨询中心',
# '宁波涌东今日科技有限公司',
# '北京润航商业发展有限公司大兴分公司',
# '英哈玩具贸易（南京）有限公司',
# '重庆西西弗文化传播有限公司',
# '贵州西西弗文化传播有限公司',
# '孩子王儿童用品股份有限公司采购中心',
# '南京爱满家贸易有限公司',
# '上海欢亚贸易有限公司',
# '上海小螺蛳教育科技有限公司',
# '北京新意乐得商贸有限公司',
# '杭州一礼文化科技有限公司',
# '四川达创商贸有限公司',
# '上海木木生活贸易有限公司',
# '青岛新华书店有限责任公司'
# ]

# 销售出库
df_saleshipping_cp = df_saleshipping[(df_saleshipping['wuliaomc'].isin(df_saleorders_dl0['wuliaomc'].drop_duplicates().to_list()))]





# 自研产品
# *********************************************************************************************************************************
list_yy = ['JAKKS','万代','游器物','乐童','汪汪队','小小王国-天天富翁超大豪华版','小猪班克儿童饮水习惯培养杯'] 




# 销售订单
# 其他公司
df_saleorders_yjy0 = df_saleorders[(df_saleorders['kehumc'].isin(list_kh))&(~df_saleorders['kehumc'].isin(['重庆西西弗文化传播有限公司','贵州西西弗文化传播有限公司','杭州迷思文化创意有限公司-西西弗']))]
# 西西弗
df_saleorders_xxf1 = df_saleorders[(df_saleorders['kehumc'].isin(['重庆西西弗文化传播有限公司','贵州西西弗文化传播有限公司']))&(df_saleorders['company'] == '杭州迷思文化创意有限公司')&(df_saleorders['wuliaomc'].isin(list(list_goods['wuliaomc'])))&(df_saleorders['riqi'] >=datetime(2021,5,1) )]
df_saleorders_xxf2 = df_saleorders[(df_saleorders['kehumc'].isin(['重庆西西弗文化传播有限公司','贵州西西弗文化传播有限公司','杭州迷思文化创意有限公司-西西弗']))&(df_saleorders['company'] == '杭州游卡文化创意有限公司')]
df_saleorders_xxf3 = df_saleorders[(df_saleorders['company']=='杭州迷思文化创意有限公司')&(df_saleorders['riqi']==datetime(2021,4,23))]

df_saleorders_t = pd.concat([df_saleorders_xxf1,df_saleorders_xxf2,df_saleorders_xxf3,df_saleorders_xxf_2018[df_saleorders_xxf_2018['wuliaomc']!='小小王国-天天富翁超大豪华版'],df_saleorders_yjy0])

# 去掉代理产品及其他
list_yy2 = set([k for k in df_saleorders_t ['wuliaomc'] if any(v1 for v1 in list_yy if v1 in k)])
df_saleorders_t  = df_saleorders_t [(~df_saleorders_t ['wuliaomc'].isin(list_yy2))&(df_saleorders_t ['wuliaomc']!='H7-旅行的蛙酱')]



# 自研产品列表
list_zy = df_saleorders_t['wuliaomc'].drop_duplicates().to_list()


# 销售出库
df_shipping_zy = df_saleshipping[(df_saleshipping['kehumc'].isin(list_kh))&(df_saleshipping['wuliaomc'].isin(list_zy))]

# 去掉代理产品
list_yy1 = set([k for k in df_shipping_zy['wuliaomc']  if any(v1 for v1 in list_yy if v1 in k)])
df_shipping_zy = df_shipping_zy[~df_shipping_zy['wuliaomc'].isin(list_yy1)]




# 退货数据\直接调拨数据（客户仓退货）
# 其他公司
df_salereturn_t = df_salereturn[(df_salereturn['kehumc'].isin(list_kh))&(df_salereturn['wuliaomc'].isin(list_zy))&(df_salereturn['company'] == '杭州游卡文化创意有限公司')]
# 西西弗
df_allocation_t1 = df_allocation[((df_allocation['diaochubgzmc'].isin(['重庆西西弗文化传播有限公司','贵州西西弗文化传播有限公司','四川言几又供应链管理有限公司']))|(df_allocation['guanlianxskh'].isin(['重庆西西弗文化传播有限公司','贵州西西弗文化传播有限公司','四川言几又供应链管理有限公司'])))]
df_allocation_t2 = df_allocation[(df_allocation['riqi'] == datetime(2021,5,7) )&(df_allocation['diaochubgzmc']=='杭州游卡文化创意有限公司')]
df_allocation_t3 = df_allocation[(df_allocation['riqi'] < datetime(2021,4,20) )&(df_allocation['diaochubgzmc']=='杭州迷思文化创意有限公司-西西弗')]
df_th = pd.concat([df_allocation_t1,df_allocation_t2,df_allocation_t3])

df_th_dl = df_th[df_th['wuliaomc'].isin(df_saleorders_dl0['wuliaomc'].drop_duplicates().to_list())]
df_th_zy = df_th[df_th['wuliaomc'].isin(df_saleorders_t['wuliaomc'].drop_duplicates().to_list())]
df_th_dl.loc[:,'属性'] = '代理'
df_th_zy.loc[:,'属性'] = '自研'
df_th = pd.concat([df_th_dl,df_th_zy])





# 客户应收账款
# *********************************************************************************************************************************
df_closebalance['日期'] = pd.to_datetime(df_closebalance['日期'],format='%Y-%m-%d')
df_closebalance = df_closebalance[(df_closebalance['客户名称'].isin(list_kh))&(df_closebalance['账簿'].isin(['杭州迷思文化创意有限公司','杭州游卡文化创意有限公司']))]

list_ = []
for j in df_closebalance['账簿'].drop_duplicates():
    for i in df_closebalance['客户名称'].drop_duplicates():
        dfcb_01 = df_closebalance[(df_closebalance['账簿']==j)&(df_closebalance['客户名称']==i)]
        dfcb_01.sort_values(['日期','审核状态'],ascending=True,inplace=True)
        dfcb_01.reset_index(drop=True,inplace=True)
        try:
            dfcb_01 = dfcb_01.iloc[dfcb_01[dfcb_01['审核状态'] == '期初'].index[0]:]
            list_.append(dfcb_01)
        except:
            list_.append(dfcb_01)

df_cb = pd.concat(list_,ignore_index=True)

# 期末余额
df_end = df_cb.groupby(['账簿','客户名称','客户编码'],as_index=False).agg({'借方金额':'sum','贷方金额':'sum'})
df_end ['余额'] = df_end ['借方金额']-df_end['贷方金额']

# 借贷明细
df_mx = df_cb.groupby(['账簿','客户名称','客户编码','日期'],as_index=False).agg({'借方金额':'sum','贷方金额':'sum'})
df_mx['rank'] = df_mx.groupby(['账簿','客户名称','客户编码'])['日期'].rank(ascending=True, method='first') 
df_mx['余额'] = df_mx['借方金额']-df_mx['贷方金额']
df_mx['累计余额'] = df_mx.groupby(['账簿','客户名称','客户编码'])['余额'].cumsum()
df_mx['余额方向'] = df_mx['累计余额'].map(lambda x:"借" if x>0 else ("平" if x==0 else "贷"))
df_mx['借方累计余额'] = df_mx.groupby(['账簿','客户名称','客户编码'])['借方金额'].cumsum()
df_mx['贷方累计余额'] = df_mx.groupby(['账簿','客户名称','客户编码'])['贷方金额'].cumsum()
# 标记最近一次发生日
list_b = []
for i in df_mx['客户编码'].drop_duplicates():
    b = df_mx[df_mx['客户编码']==i]
    list_b.append(b[b['rank']==b['rank'].max()])
df_markend = pd.concat(list_b)

# 连接
df_markend.rename(columns={'日期':'最后一次发生日期'},inplace=True)
df_mx = df_mx.join(df_markend['最后一次发生日期'])


# 计算每笔结余
def surplus(df_mx):
    list_ab = []
    for i in df_mx['客户编码'].drop_duplicates():
        
        a = df_mx[df_mx['客户编码']==i]
        
        if list(a['余额方向'])[-1] == '借':
            # 贷方金额总合计
            b = a['贷方金额'].sum()
            # 冲借方金额
            list_lj = []
            a.reset_index(drop=True,inplace=True)
            for i in range(len(a)):
                if b >= a['借方金额'][i]:
                    list_lj.append(0)
                    b = b-a['借方金额'][i]
                elif b < a['借方金额'][i] and b != 0:
                    list_lj.append(a['借方金额'][i]-b)
                    b = 0
                else:
                    list_lj.append(a['借方金额'][i])
            a.loc[:,'结余金额'] = pd.DataFrame(list_lj)
            list_ab.append(a)
        
        elif list(a['余额方向'])[-1] == '贷':
            # 借方金额总合计
            b = a['借方金额'].sum()
            # 冲贷方金额
            list_lj = []
            a.reset_index(drop=True,inplace=True)
            for i in range(len(a)):
                if b >= a['贷方金额'][i]:
                    list_lj.append(0)
                    b = b-a['贷方金额'][i]
                elif b < a['贷方金额'][i] and b != 0:
                    list_lj.append(a['贷方金额'][i]-b)
                    b = 0
                else:
                    list_lj.append(a['贷方金额'][i])
            a.loc[:,'结余金额'] = pd.DataFrame(list_lj)
            list_ab.append(a)
        else:
            pass
    df = pd.concat(list_ab)
    return df


df_mx1 = surplus(df_mx)


df_mx1['日期'] = pd.to_datetime(df_mx1['日期'],format='%Y-%m-%d')
df_mx1['账龄'] = datetime.now()-df_mx1['日期']
df_mx1['账龄'] = df_mx1['账龄'].map(lambda x:int(str(x).split(' ')[0]) if str(x) != 'NaT' else x)
df_mx1.reset_index(drop=True,inplace=True)
df_mx1 = df_mx1.join(pd.DataFrame(df_mx1[df_mx1['贷方金额']>0]['日期']).rename(columns={'日期':'回款日期'}))


# 回款时长字段应往下对其一行，每个客户最后一个回款时长为至月末未回款天数
list_mx = []

for i in list_kh:
# for i in list_customers:
    df_mx_ = df_mx1[df_mx1['客户名称'] == i]
    if df_mx_.empty == False:
        df_mx_['回款日期'].fillna(method='ffill',inplace=True)
        df_mx_['回款日期'].fillna(df_mx_['日期'],inplace=True)
        df_mx_.reset_index(drop=True,inplace=True)

        list_a = []
        list_b = []
        for j in range(df_mx_.shape[0]):
            try:
                list_a.append(df_mx_['回款日期'][j+1]-df_mx_['回款日期'][j]) 
                list_b.append('回款时长')    
            except:
                list_a.append(datetime.now()-df_mx_['回款日期'][j])
                list_b.append('最近回款距今天数')
        df_mx_.loc[:,'回款时长'] = pd.DataFrame(list_a)
        df_mx_.loc[:,'回款类型'] = pd.DataFrame(list_b)
        list_mx.append(df_mx_)
df_mx1 = pd.concat(list_mx,ignore_index=True)

df_mx1['回款时长'] = df_mx1['回款时长'].map(lambda x: int(str(x).split(' ')[0]))


df_mx_01 = df_mx1[(df_mx1['回款时长']>0)&(df_mx1['回款类型']!='最近回款距今天数')].groupby(['客户名称']).agg({'回款时长':'mean','日期':'count'})
df_mx_02 = df_mx1[(df_mx1['贷方金额']>0)].groupby(['客户名称']).agg({'贷方金额':'mean'})
df_mx_02.rename(columns={'贷方金额':'平均回款金额（次）'},inplace=True)
df_mx_03 = df_mx1.groupby(['客户名称']).agg({'借方金额':'sum','贷方金额':'sum'})
df_mx_04 = df_mx1[df_mx1['回款类型']=='最近回款距今天数'].groupby(['客户名称']).agg({'回款时长':'sum'})
df_mx_04.rename(columns={'回款时长':'最近回款距今天数'},inplace=True)

df_mx_s = df_mx_01.join(df_mx_02)
df_mx_s = df_mx_s.join(df_mx_03)
df_mx_s = df_mx_s.join(df_mx_04)
df_mx_s['应收款项'] = round(df_mx_s['借方金额'] - df_mx_s['贷方金额'],0)
df_mx_s['平均回款周期（天）'] = round(df_mx_s['回款时长'],0)
df_mx_s['平均回款金额（次）'] = round(df_mx_s['平均回款金额（次）'],0)
df_mx_s['借方金额'] = round(df_mx_s['借方金额'],0)
df_mx_s['贷方金额'] = round(df_mx_s['贷方金额'],0)
df_mx_s['回款次数'] = round(df_mx_s['日期'],0)







# 代理产品资金占用
# *********************************************************************************************************************************

df_saleorders_dl = df_saleorders_dl0.groupby(['wuliaomc'],as_index=False).agg({'xiaoshousl':'sum','jiashuihj':'sum'})
df_saleshipping_dl = df_saleshipping_cp.groupby(['wuliaomc'],as_index=False).agg({'shifasl':'sum','jiashuihj':'sum'})


# 
df_fh = pd.merge(df_saleorders_dl,df_saleshipping_dl,on = ['wuliaomc'],how = 'left')
df_fh.rename(columns={'wuliaomc':'wlmc_all','shifasl':'销量','jiashuihj_x':'发货金额','xiaoshousl':'发货数量','jiashuihj_y':'销售额'},inplace=True)
df_cg = df_purchaseorders_ms.groupby(['品牌','wlmc_all'],as_index=False).agg({'上市月份':'max','零售价':'max','caigousl_new':'sum','jiashuihj':'sum'})
df_total1 = pd.merge(df_cg,df_fh,on = ['wlmc_all'],how = 'left')

df_total1 = pd.merge(df_total1,df_purchasecost[['wlmc_all','cost']],on = ['wlmc_all'],how = 'left')
df_total = df_total1.groupby(['品牌','wlmc_all'],as_index=False).agg({'上市月份':'max', 'caigousl_new':'sum', 'jiashuihj':'sum', '发货数量':'sum', '发货金额':'sum','销量':'sum', '销售额':'sum', 'cost':'mean'})
df_total.rename(columns={'wlmc_all':'产品名称','caigousl_new':'采购数量', 'jiashuihj':'采购金额','cost':'采购单价'},inplace=True)

df_total = pd.merge(df_total,df_pricelist[['品名','商品定价','供货价']],left_on=['产品名称'],right_on=['品名'],how='left')
df_total.drop(['品名'],axis=1,inplace=True)



# 字段计算
df_total['供货价'] = df_total['供货价'].astype(float)
df_total['库存结余数量'] = df_total['发货数量']-df_total['销量']
df_total['库存结余金额'] = df_total['库存结余数量']*df_total['供货价']
df_total['库存结余成本'] = df_total['库存结余数量']*df_total['采购单价']
df_total['预估总产出'] = df_total['供货价']*df_total['采购数量']
df_total['预估毛利'] = df_total['预估总产出']-df_total['采购金额']
df_total['出货成本'] = df_total['采购单价']*df_total['发货数量']
df_total['实际成本'] = df_total['销量']*df_total['采购单价']
df_total['实际毛利'] = df_total['销售额']-df_total['实际成本']
df_total['实际毛利率'] = df_total['实际毛利']/df_total['销售额']
df_total['预估毛利率'] = df_total['预估毛利']/df_total['预估总产出']
df_total['月均销售'] = df_total['销量']/df_total['上市月份']



df_total_s1 = df_total.groupby(['品牌']).agg({'产品名称':'count','销量':'sum','发货数量':'sum','发货金额':'sum','采购金额':'sum','销售额':'sum','实际成本':'sum','库存结余数量':'sum','月均销售':'sum'})
df_total_s1.rename(columns={'销售额':'实际出货收入(e)','销量':'实际出货量','实际成本':'实际出货成本(b)','产品名称':'SKU数量','采购金额':'实际采购成本(a)'},inplace=True)



# 固定利率
r = 0.0435
# 计算资金占用
df_purchaseorders_ms.loc[:,'year'] = df_purchaseorders_ms['year'].astype(int)
df_purchaseorders_ms.loc[:,'month'] = df_purchaseorders_ms['month'].astype(int)

df_saleshipping_cp.loc[:,'year'] = df_saleshipping_cp['year'].astype(int)
df_saleshipping_cp.loc[:,'month'] = df_saleshipping_cp['month'].astype(int)

df_saleshipping_cp.loc[:,'品牌'] = df_saleshipping_cp['wuliaomc'].map(lambda x :getdictkey(dict_pp,x))

# 计算资金占用成本
def funck(name):
    df_zj_js = df_saleshipping_cp.rename(columns={'wuliaomc':'wlmc_all'}).groupby([name,'year','month'],as_index = False).agg({'jiashuihj':'sum'})
    df_zj_cg = df_purchaseorders_ms.groupby([name,'year','month'],as_index = False).agg({'jiashuihj':'sum'})
    df_zj_cg.rename(columns={'jiashuihj':'采购金额'},inplace=True)

    df_zj = pd.merge(df_zj_js,df_zj_cg,on = [name,'year','month'],how='outer')

    # 补充缺失月份，计算至月末
    list_ym = []
    for i in df_zj[name].drop_duplicates():
        df_zj1 = df_zj[df_zj[name]==i]
        df_zj1.sort_values(['year','month'],inplace=True)
        df_zj1.reset_index(drop=True,inplace=True)
        df_ym = func(df_zj1.loc[0,'year'],datetime.now().year)[~(((func(df_zj1.loc[0,'year'],datetime.now().year)['year'] ==datetime.now().year)&(func(df_zj1.loc[0,'year'],datetime.now().year)['month'] >datetime.now().month))|((func(df_zj1.loc[0,'year'],datetime.now().year)['year'] ==df_zj1.loc[0,'year'])&(func(df_zj1.loc[0,'year'],datetime.now().year)['month'] <df_zj1.loc[0,'month'])))]
        df_zj1 = pd.merge(df_ym,df_zj1,on=['year','month'],how='left')
        df_zj1[name].fillna(method='ffill',inplace=True)
        list_ym.append(df_zj1)
    df_zj = pd.concat(list_ym,ignore_index=True)
    df_zj.fillna(0,inplace=True)
    df_zj['资金占用'] = df_zj['采购金额']-df_zj['jiashuihj']
    df_zj['资金占用'] = df_zj['资金占用'].map(lambda x:x if x>0 else 0)
    df_zj['累计资金占用'] = df_zj.groupby(name)['资金占用'].cumsum()
    df_zj['资金占用成本(d)'] = df_zj['累计资金占用']*r/12
    df_zj_s = df_zj.reset_index().groupby([name]).agg({'资金占用成本(d)':'sum'})
    df_zj_s.loc['合计'] = df_zj_s.sum(axis=0)

    return df_zj_s

df_zj_s = funck('品牌')
df_zj_s_wl = funck('wlmc_all')


df_total_s1 = df_total_s1.join(df_zj_s)


df_total['未出货数量'] = df_total['采购数量'] - df_total['销量']
df_total['剩余销售时间'] = df_total['未出货数量']/df_total['月均销售']
# 异常值变为1
df_total['剩余销售时间'] = df_total['剩余销售时间'].map(lambda x: x if x < 100 else 1)

df_total['未出货成本'] = df_total['采购金额'] - df_total['实际成本']
df_total['资金占用成本'] = df_total['剩余销售时间']*df_total['未出货成本']
df_total['假设今日起至预期完成销售预估资金占用天数(n)'] = round(df_total['资金占用成本']/df_total['未出货成本']/2*30+36,0)
df_total['未来资金占用成本'] = round(df_total['假设今日起至预期完成销售预估资金占用天数(n)']*df_total['未出货成本']*r/360,0)


df_zj_t = df_total.groupby(['品牌']).agg({'资金占用成本':'sum','未出货成本':'sum','剩余销售时间':'sum'})
df_zj_t.loc['合计'] = df_zj_t.sum(axis=0)
df_zj_t['假设今日起至预期完成销售预估资金占用天数(n)'] = round(df_zj_t['资金占用成本']/df_zj_t['未出货成本']/2*30+36,0)
df_total_s1 = df_total_s1.join(df_zj_t['假设今日起至预期完成销售预估资金占用天数(n)'])

df_total_s1.fillna(0,inplace=True)


df_total_s1['资金占用(c=a-b)'] = round(df_total_s1['实际采购成本(a)']-df_total_s1['实际出货成本(b)'],0)
df_total_s1['资金占用天数(h=d/(a*r)*365)'] = round(df_total_s1['资金占用成本(d)']/(df_total_s1['实际采购成本(a)']*r)*365,0)
df_total_s1['截至今日占用资金总额'] = round(df_total_s1['资金占用成本(d)']+df_total_s1['资金占用(c=a-b)'],0)
df_total_s1['截至今日实现盈亏(f=e-a-d)'] = round(df_total_s1['实际出货收入(e)']-df_total_s1['资金占用成本(d)']-df_total_s1['实际采购成本(a)'],0)
df_total_s1['资金占用成本(g=(a-b)*4.35%*n/360)'] = round(df_total_s1['假设今日起至预期完成销售预估资金占用天数(n)']*df_total_s1['资金占用(c=a-b)']*r/360,0)
df_total_s1['预期销售收入'] = round(df_total_s1['发货金额']-df_total_s1['实际出货收入(e)'],0)
df_total_s1['实际出货毛利(e-b)'] = round(df_total_s1['实际出货收入(e)']-df_total_s1['实际出货成本(b)'],0)
df_total_s1['预期盈亏情况'] = round(df_total_s1['预期销售收入']+df_total_s1['截至今日实现盈亏(f=e-a-d)'],0)
df_total_s1['预期总资金占用天数'] = round((df_total_s1['资金占用成本(d)']+df_total_s1['资金占用成本(g=(a-b)*4.35%*n/360)'])/df_total_s1['实际采购成本(a)']*360/r,0)

df_total_s1['动销率'] = round(df_total_s1['实际出货量']/df_total_s1['发货数量'],2)
df_total_s1[df_total_s1['资金占用天数(h=d/(a*r)*365)']<0]['资金占用天数(h=d/(a*r)*365)'] = 0
df_total_s1[df_total_s1['资金占用成本(g=(a-b)*4.35%*n/360)']<0]['资金占用成本(g=(a-b)*4.35%*n/360)'] = 0




# *********************************************************************************************************************************
df_th. to_sql('qd_allocation_zy',   engine, schema='erp_jd_dws', if_exists='replace',index=False)
df_salereturn_t. to_sql('qd_salereturn_zy',   engine, schema='erp_jd_dws', if_exists='replace',index=False)
df_saleorders_t. to_sql('qd_saleorders_zy',   engine, schema='erp_jd_dws', if_exists='replace',index=False)
df_shipping_zy. to_sql('qd_saleshipping_zy',   engine, schema='erp_jd_dws', if_exists='replace',index=False)
df_purchaseorders_ms. to_sql('qd_purchaseorders_dl',   engine, schema='erp_jd_dws', if_exists='replace',index=False)
df_saleorders_dl0. to_sql('qd_saleorders_dl',   engine, schema='erp_jd_dws', if_exists='replace',index=False)
df_saleshipping_cp. to_sql('qd_saleshipping_dl',   engine, schema='erp_jd_dws', if_exists='replace',index=False)
df_mx_s[['平均回款周期（天）','回款次数','平均回款金额（次）','借方金额','贷方金额','最近回款距今天数','应收款项']].reset_index(). to_sql('dlzy_proceeds',   engine, schema='www_bi_ads', if_exists='replace',index=False)
df_total_s1.reset_index(). to_sql('dlzy_cash_cost',   engine, schema='www_bi_ads', if_exists='replace',index=False)
df_total.to_sql('dlzy_cash_cost_mx',   engine, schema='www_bi_ads', if_exists='replace',index=False)
df_zj_s_wl.reset_index().to_sql('dlzy_cash_cost_mx1',   engine, schema='www_bi_ads', if_exists='replace',index=False)



print("\n","END qd_dlzy",datetime.now(),"\n")