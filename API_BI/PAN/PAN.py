# -*- coding: utf-8 -*-
# 测试环境: python3.10.1




# 实例托盘
class pan:

    # 托盘规格
    L = 120
    W = 100
    S = L*W


    def __init__(self,l,w):
        self.l = l
        self.w = w


    # 定义变量为箱规长宽，A摆放
    def funcA(self):
        n = int(self.L/self.l)
        m = int(self.W/self.w)
        text = '行：'+str(m)+'，列：'+str(n)
        return n*m,text 

    # 定义变量为箱规长宽，B摆放
    def funcB(self):
        n = int(self.L/self.w)
        m = int(self.W/self.l)
        text = '行：'+str(m)+'，列：'+str(n)
        return n*m,text


    # 定义变量为箱规长宽，C摆放
    def funcC(self):
        import pulp

        MyProbLP = pulp.LpProblem("LPProbDemo1", sense=pulp.LpMinimize)  
        m = pulp.LpVariable('m', lowBound=1, upBound=20, cat='Integer') 
        n = pulp.LpVariable('n', lowBound=1, upBound=20, cat='Integer') 
        p = pulp.LpVariable('p', lowBound=1, upBound=20, cat='Integer') 
        q = pulp.LpVariable('q', lowBound=1, upBound=20, cat='Integer') 
        MyProbLP += m*self.l-q*self.w+p*self.l-n*self.w	# 设置目标函数
        MyProbLP += (q*self.w+m*self.l <= self.L)  
        MyProbLP += (n*self.w+p*self.l <= self.W)  
        MyProbLP += (m*self.l-q*self.w >= 0)  
        MyProbLP += (p*self.l-n*self.w >= 0) 
        MyProbLP += (p*self.l-n*self.w+1 <= self.w) 
        MyProbLP.solve()  
        m = MyProbLP.variables()[0].varValue
        n = MyProbLP.variables()[1].varValue
        p = MyProbLP.variables()[2].varValue
        q = MyProbLP.variables()[3].varValue

        text = 'A区行：'+str(n)+'，列：'+str(m)+'；B区行：'+str(p)+'，列：'+str(q)

        return int(n*m*2+p*q*2),text


    # 定义变量为箱规长宽，D摆放
    # 分为上下两个区域，上区域n*m,下区域q*p, n,q为层数
    def funcD(self):
        try:
            listN = []
            listm = []
            listn = []
            listq = []
            listp = []
            n = 0
            m = int(self.L/self.l)
            p = int(self.L/self.w)
            for i in range(1,10):
                if self.W-self.w*i>=self.l:
                    n += 1
                    q = int((self.W-self.w*i)/self.l)
                    N = n*m+q*p
                    listN.append(N)
                    listm.append(m)
                    listn.append(n)
                    listq.append(q)
                    listp.append(p)
                else:
                    break
            df = pd.DataFrame(np.array([listN,listm,listn,listq,listp]).T,columns=['盒数','长边数量','上层层数','下层层数','短边数量'])
            N = df[df['盒数']==df['盒数'].max()].reset_index(drop=True)['盒数'][0]
            m = df[df['盒数']==df['盒数'].max()].reset_index(drop=True)['长边数量'][0]
            n = df[df['盒数']==df['盒数'].max()].reset_index(drop=True)['上层层数'][0]
            q = df[df['盒数']==df['盒数'].max()].reset_index(drop=True)['下层层数'][0]
            p = df[df['盒数']==df['盒数'].max()].reset_index(drop=True)['短边数量'][0]


            text = '上层行：'+str(n)+'，列：'+str(m)+'；下层行：'+str(q)+'，列：'+str(p)
            return N, text
        except:
            return 0, '无'


    # 定义变量为箱规长宽，E摆放
    # 与D长短边置换
    def funcE(self):
        try:
            listN = []
            listm = []
            listn = []
            listq = []
            listp = []
            n = 0
            m = int(self.W/self.l)
            p = int(self.W/self.w)
            for i in range(1,10):
                if self.L-self.w*i>=self.l:
                    n += 1
                    q = int((self.W-self.w*i)/self.l)
                    N = n*m+q*p
                    listN.append(N)
                    listm.append(m)
                    listn.append(n)
                    listq.append(q)
                    listp.append(p)
                else:
                    break
            df = pd.DataFrame(np.array([listN,listm,listn,listq,listp]).T,columns=['盒数','长边数量','上层层数','下层层数','短边数量'])
            N = df[df['盒数']==df['盒数'].max()].reset_index(drop=True)['盒数'][0]
            m = df[df['盒数']==df['盒数'].max()].reset_index(drop=True)['长边数量'][0]
            n = df[df['盒数']==df['盒数'].max()].reset_index(drop=True)['上层层数'][0]
            q = df[df['盒数']==df['盒数'].max()].reset_index(drop=True)['下层层数'][0]
            p = df[df['盒数']==df['盒数'].max()].reset_index(drop=True)['短边数量'][0]

            text = '左侧行：'+str(m)+'，列：'+str(n)+'；右侧行：'+str(p)+'，列：'+str(q)
            return N,text
        except:
            return 0, '无'



    def result(self):
        if self.funcA()[0]==max(self.funcA()[0],self.funcB()[0],self.funcC()[0],self.funcD()[0],self.funcE()[0]):
            return self.funcA()[0],'A',self.funcA()[1]
        elif self.funcB()[0]==max(self.funcA()[0],self.funcB()[0],self.funcC()[0],self.funcD()[0],self.funcE()[0]):
            return self.funcB()[0],'B',self.funcB()[1]
        elif self.funcC()[0]==max(self.funcA()[0],self.funcB()[0],self.funcC()[0],self.funcD()[0],self.funcE()[0]):
            return self.funcC()[0],'C',self.funcC()[1]
        elif self.funcD()[0]==max(self.funcA()[0],self.funcB()[0],self.funcC()[0],self.funcD()[0],self.funcE()[0]):
            return self.funcD()[0],'D',self.funcD()[1]
        elif self.funcE()[0]==max(self.funcA()[0],self.funcB()[0],self.funcC()[0],self.funcD()[0],self.funcE()[0]):
            return self.funcE()[0],'E',self.funcE()[1]




# 调数据源
import pandas as pd
import math
from datetime import datetime
from sqlalchemy import create_engine,text


# *****************************************连接mysql、sql server*****************************************#
engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format('root', '123456', 'localhost', '3306'))  

# 去掉残次品
df_kc = pd.read_sql_query(text(
"""SELECT b.wuliaomc ,sum(b.inventory) 库存, 
a.classify,	a.classify_1,	a.classify_2, a.chang,	a.kuan,	a.gao,	a.danxiangbzsl 
FROM erp_jd_dws.erp_jd_dws_warehouse b
left join (
SELECT classify,	classify_1,	classify_2,	wuliaomc,chang,	kuan,	gao,	danxiangbzsl 
FROM erp_jd_dwd.erp_jd_dwd_fact_classify
where chang <> 0 and danxiangbzsl<>0
) a on a.wuliaomc = b.wuliaomc
where cangkumc not like '%%残次%%'
group by wuliaomc 
having sum(inventory)>0
;"""), engine.connect())

df_part1 = df_kc.dropna().reset_index(drop=True)

df_part1['箱数'] = df_part1['库存']/df_part1['danxiangbzsl']
df_part1['箱数'] = df_part1['箱数'].map(lambda x:math.ceil(x))



listx1 = []
listx2 = []
listx3 = []
for i in range(len(df_part1)):
    x = pan(df_part1['chang'][i],df_part1['kuan'][i])
    listx1.append(x.result()[0])
    listx2.append(x.result()[1])
    listx3.append(x.result()[2])

df_part1['可放置数量'] = pd.DataFrame(listx1)
df_part1['摆放方式'] = pd.DataFrame(listx2)
df_part1['摆放方式（数字）'] = pd.DataFrame(listx3)



import math


# 托盘限高，可改动
df_part1.loc[:,'限高'] = 160

df_part1['层数'] = df_part1['限高']/df_part1['gao']
df_part1['层数'] = df_part1['层数'].map(lambda x:int(x))

df_part1['托盘数'] = df_part1['箱数']/(df_part1['可放置数量']*df_part1['层数'])

# 余量产品可拼在一个托盘
listc = []
for i in range(len(df_part1)):
    if df_part1['箱数'][i] < df_part1['可放置数量'][i]:
        df_part1['托盘数'][i] = 1
        listc.append('可拼')
    else:
        listc.append('不可拼')

df_part1['是否可拼托盘'] = pd.DataFrame(listc)

df_part1['托盘数'] = df_part1['托盘数'].map(lambda x:math.ceil(x))
df_part1['平面面积'] = df_part1['chang']*df_part1['kuan']


df_part1.to_sql('inventory_pan', engine, schema='www_bi_ads', if_exists='replace',index=False) 