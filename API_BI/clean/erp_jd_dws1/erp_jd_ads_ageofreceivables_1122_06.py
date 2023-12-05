import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

import pandas as pd
from sqlalchemy import create_engine,text
from datetime import datetime 
from key_tab import getDict


# *****************************************连接mysql、sql server*****************************************#
engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format('root', '123456', 'localhost', '3306'))
df_closebalance = pd.read_sql_query(text("select * from erp_jd_dws.erp_jd_dws_closebalance where 科目编码 = '1122.06' and 客户编码 is not null and 账簿 is not null and 审核状态<>'创建';"), engine.connect())

engine.dispose()

# df_ageofreceivables   账龄表
# ----------------------------------------------------------------------------------------------------- # 
df_closebalance.fillna(0,inplace=True)


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

# 处理借贷方向
df_mx['借方金额1'] = df_mx['借方金额']
df_mx['贷方金额1'] = df_mx['贷方金额']
for i in range(len(df_mx)):
    if df_mx['借方金额1'][i]<0:
        df_mx.loc[i,'贷方金额1'] = df_mx['贷方金额1'][i]-df_mx['借方金额1'][i]
        df_mx.loc[i,'借方金额1'] = 0
    elif df_mx['贷方金额1'][i]<0:
        df_mx.loc[i,'借方金额1'] = df_mx['借方金额1'][i]-df_mx['贷方金额1'][i]
        df_mx.loc[i,'贷方金额1'] = 0   


df_mx['余额'] = df_mx['借方金额1']-df_mx['贷方金额1']
df_mx['累计余额'] = df_mx.groupby(['账簿','客户名称','客户编码'])['余额'].cumsum()
df_mx['余额方向'] = df_mx['累计余额'].map(lambda x:"借" if x>0 else ("平" if x==0 else "贷"))
df_mx['借方累计余额'] = df_mx.groupby(['账簿','客户名称','客户编码'])['借方金额1'].cumsum()
df_mx['贷方累计余额'] = df_mx.groupby(['账簿','客户名称','客户编码'])['贷方金额1'].cumsum()
# 标记最近一次发生日
list_b = []
for j in df_mx['账簿'].drop_duplicates():
    for i in df_mx['客户编码'].drop_duplicates():
        b = df_mx[(df_mx['客户编码']==i)&(df_mx['账簿']==j)]
        list_b.append(b[b['rank']==b['rank'].max()])
df_markend = pd.concat(list_b)

# 连接
df_markend.rename(columns={'日期':'最后一次发生日期'},inplace=True)
df_mx = df_mx.join(df_markend['最后一次发生日期'])




# 计算每笔结余
def surplus(df_mx):
    list_ab = []
    for j in df_mx['账簿'].drop_duplicates():
        for i in df_mx['客户编码'].drop_duplicates():
            
            a = df_mx[(df_mx['客户编码']==i)&(df_mx['账簿']==j)]
            try:
                if list(a['余额方向'])[-1] == '借':
                    # 贷方金额总合计
                    b = a['贷方金额1'].sum()
                    # 冲借方金额
                    list_lj = []
                    a.reset_index(drop=True,inplace=True)
                    for i in range(len(a)):
                        if b >= a['借方金额1'][i]:
                            list_lj.append(0)
                            b = b-a['借方金额1'][i]
                        elif b < a['借方金额1'][i] and b != 0:
                            list_lj.append(a['借方金额1'][i]-b)
                            b = 0
                        else:
                            list_lj.append(a['借方金额1'][i])
                    a.loc[:,'结余金额'] = pd.DataFrame(list_lj)
                    list_ab.append(a)
                
                elif list(a['余额方向'])[-1] == '贷':
                    # 借方金额总合计
                    b = a['借方金额1'].sum()
                    # 冲贷方金额
                    list_lj = []
                    a.reset_index(drop=True,inplace=True)
                    for i in range(len(a)):
                        if b >= a['贷方金额1'][i]:
                            list_lj.append(0)
                            b = b-a['贷方金额1'][i]
                        elif b < a['贷方金额1'][i] and b != 0:
                            list_lj.append(a['贷方金额1'][i]-b)
                            b = 0
                        else:
                            list_lj.append(a['贷方金额1'][i])
                    a.loc[:,'结余金额'] = pd.DataFrame(list_lj)
                    list_ab.append(a)
            except:
                pass
    df = pd.concat(list_ab)
    return df


df_mx1 = surplus(df_mx)


df_mx1['日期'] = pd.to_datetime(df_mx1['日期'],format='%Y-%m-%d')
df_mx1['账龄'] = datetime.now()-df_mx1['日期']
df_mx1['账龄'] = df_mx1['账龄'].map(lambda x:int(str(x).split(' ')[0]) if str(x) != 'NaT' else x)

df_ageofreceivables = df_mx1[df_mx1['结余金额']>0][['账簿','客户名称','客户编码','日期','借方金额','贷方金额','余额','余额方向','结余金额','账龄']]
dict_label = {
                '0-30天':[0,30],
                '31-60天':[30,60],
                '61-90天':[60,90],
                '91-180天':[90,180],
                '181-360天':[180,360],
                '360天以上':[360,10000]}

df_ageofreceivables['账龄区间'] = df_ageofreceivables['账龄'].map(lambda x:getDict(dict_label,x,'无'))


# *****************************************数据更新增加*****************************************************# 
df_ageofreceivables['refresh'] = datetime.now()


# *****************************************写入mysql*****************************************************#
savesql(df_ageofreceivables,'erp_jd_ads','erp_jd_ads_ageofreceivables_1122_06',"""CREATE TABLE `erp_jd_ads_ageofreceivables_1122_06` (
  `账簿` text,
  `客户名称` text,
  `客户编码` text,
  `日期` datetime DEFAULT NULL,
  `借方金额` double DEFAULT NULL,
  `贷方金额` double DEFAULT NULL,
  `余额` double DEFAULT NULL,
  `余额方向` text,
  `结余金额` double DEFAULT NULL,
  `账龄` bigint DEFAULT NULL,
  `账龄区间` text,
  `refresh` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;""",
"INSERT INTO erp_jd_ads_ageofreceivables_1122_06(账簿,客户名称,客户编码,日期,借方金额,贷方金额,余额,余额方向,结余金额,账龄,账龄区间,refresh) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")