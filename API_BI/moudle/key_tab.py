# -*- coding: utf-8 -*-
# 测试环境: python3.9

import pandas as pd
from datetime import datetime
import pymysql


# 模糊匹配函数
# @df：主表
# @list_：需查询元素的列表
# @name：主表种要查询的列名
# @name1：新定义的列名
def fuzzy(df,list_,name,name1):
    # 长度排序
    list_ = sorted(list_,key = lambda i:len(i),reverse=True)  
    # 索引列
    df.reset_index(inplace=True)
    list_i = []
    list_j = []
    df_empty = pd.DataFrame()
    for i in range(len(df[name])):
        for j in list_:
            try:
                if df[name][i].__contains__(j) :
                    list_i.append(i)
                    list_j.append(j)
                    break
            except:
                continue
    df_empty['index'] = pd.DataFrame(list_i)
    df_empty[name1] = pd.DataFrame(list_j)
    df_empty.drop_duplicates('index',inplace=True)
    df_important = pd.merge(df, df_empty, on=['index'], how = 'left')
    df_important.drop(labels='index', axis=1, inplace=True)

    return df_important




# 适用采购物料与销售物料不能完全匹配的处理，返回可匹配的物料名称
# @df_purchases：主表
# @df_sale：销售表物料列表
# @column_name：原数量的列名
# @date_name：日期的列名
# @df_classify：分类表
def merge_label(df_purchases, df_sale,column_name,date_name,df_classify):

    column_names = column_name+'_new'

    # 返回原表所属年度
    df_purchases[date_name] = pd.to_datetime(df_purchases[date_name],format='%Y-%m-%d')
    df_purchases['year'] = df_purchases[date_name].map(lambda x: x.year)  
    df_purchases['month'] = df_purchases[date_name].map(lambda x: x.month)  

    df_purchase__ = df_purchases[df_purchases['wuliaomc'].isin(df_classify['wuliaomc'].to_list())].reset_index(drop=True) 
    df_purchase__ = pd.merge(df_purchase__,df_classify[['wuliaomc','wlmc_all','label']],on=['wuliaomc'],how='left')
    df_purchase__['wlmc_new'] = df_purchase__['wlmc_all']
    # 新增采购量列
    df_purchase__[column_names] = df_purchase__[column_name]
    df_purchase__.loc[df_purchase__['label']!='装配',column_names] = 0
    # 新增成品标签列   
    df_purchase__['shifoucp'] = '成品'
    df_purchase__['mark_cp'] = '成品标记'
    df_purchase__.loc[df_purchase__['label']!='装配','shifoucp'] = '半成品'
    df_purchase__.loc[df_purchase__['label']!='装配','mark_cp'] = '半成品标记'


    df_purchases1 = df_purchases[~df_purchases['wuliaomc'].isin(df_classify['wuliaomc'].to_list())].reset_index(drop=True) 
    df_purchases1['wlmc_all'] = df_purchases1['wuliaomc'].map(lambda x:x if x in df_sale else 0) 



    df_purchase_ = df_purchases1[df_purchases1['wlmc_all']!=0].reset_index(drop=True)
    df_purchase_['label'] = '装配'
    df_purchase_['wlmc_new'] = df_purchase_['wlmc_all']

    # 新增采购量列
    df_purchase_[column_names] = df_purchase_[column_name]
    # 新增成品标签列                    
    df_purchase_['shifoucp'] = '成品'
    df_purchase_['mark_cp'] = '成品标记' 

    df_purchase = df_purchases1[df_purchases1['wlmc_all']==0].reset_index(drop=True).drop(['wlmc_all'],axis=1)


    list_a = []
    list_b = []
    for i in range(len(df_purchase)):
        if df_purchase['wuliaomc'][i][-3:] == '-装配':
            list_a.append(df_purchase['wuliaomc'][i][:-3])
            list_b.append(df_purchase['wuliaomc'][i][-2:])
        elif df_purchase['wuliaomc'][i][-2:] == '装配':
            list_a.append(df_purchase['wuliaomc'][i][:-2])
            list_b.append(df_purchase['wuliaomc'][i][-2:])
        elif df_purchase['wuliaomc'][i][-4:] == '- 装配':
            list_a.append(df_purchase['wuliaomc'][i][:-4])
            list_b.append(df_purchase['wuliaomc'][i][-2:])
        else:
            list_a.append(df_purchase['wuliaomc'][i])
            list_b.append('0')

    # 打上装配标签,且将去除装配的字段作为新物料名称
    df_purchase['label'] = pd.DataFrame(list_b)  
    df_purchase['wlmc_new'] = pd.DataFrame(list_a)



    list_cnew = []
    list_cp = []
    list_bj = []
    df_purchase['wuliaobm'].fillna('0',inplace=True)
    df_purchase['wuliaofzid'].fillna('0',inplace=True)
    for i in range(len(df_purchase)):
        if df_purchase['wuliaobm'][i][:2] in ['01','03'] or df_purchase['wuliaofzid'][i][:4] in ['0207','0308'] or df_purchase['wuliaofzid'][i][:2] == '04':
            list_cnew.append(df_purchase[column_name][i])
            list_cp.append('成品')
            list_bj.append('成品标记')
        elif df_purchase['label'][i] == '装配':
            list_cnew.append(df_purchase[column_name][i])
            list_cp.append('半成品')
            list_bj.append('成品标记')
        else:
            list_cnew.append(0)
            list_cp.append('半成品')
            list_bj.append('非成品标记')


    # 新增采购量列，处理掉重复值
    df_purchase[column_names] = pd.DataFrame(list_cnew)    
    # 新增成品标签列                    
    df_purchase['shifoucp'] = pd.DataFrame(list_cp)  
    df_purchase['mark_cp'] = pd.DataFrame(list_bj)                            
    # 获取半成品的产品名称（与销售可匹配的）
    list_02 =  df_purchase[df_purchase['mark_cp'] == '成品标记']['wlmc_new'].drop_duplicates().to_list() + df_purchase_ ['wlmc_all'].drop_duplicates().to_list()   
    list_02 = sorted(list_02,key = lambda i:len(i),reverse=True) 

    df_purchase_01 = df_purchase[df_purchase['shifoucp'] == '成品'].reset_index(drop=True)  
    df_purchase_02 = df_purchase[df_purchase['shifoucp'] == '半成品'].reset_index(drop=True)                                                                      
    df_purchase_01['wlmc_all'] = df_purchase_01['wuliaomc']



    a = df_purchase_02['wuliaomc'].drop_duplicates().reset_index()
    lista = []
    for j in a['wuliaomc']:
        try:
            lista.append([i for i in list_02 if i in j][0])
        except:
            lista.append(j)
    a['wlmc_all'] = pd.DataFrame(lista)
    df_purchase_02 = pd.merge(df_purchase_02,a[['wuliaomc','wlmc_all']],on=['wuliaomc'],how='left')
    # 完整物料名称，可匹配销售
    df_purchase = pd.concat([df_purchase__,df_purchase_,df_purchase_01,df_purchase_02],ignore_index=True)  
    df_purchase[date_name] = pd.to_datetime(df_purchase[date_name].map(lambda x: str(x)[:10]), format='%Y-%m-%d')

    return df_purchase




def getDictKey(mydict,value,word):
    try:
        return [k for k,v in mydict.items() if value in v][0]
    except:
        return word


def getDict(mydict,value,word):
    try:
        return [k for k,v in mydict.items() if value > v[0] and value <= v[1]][0]
    except:
        return word


def getDictKey1(mydict,key,word):
    try:
        return [v for k,v in mydict.items() if key in k][0]
    except:
        return word
        




# 销售表的清洗函数
# @df：主表
# @dfc：年均成本表
def clean(df,dfc,department,salename):

    # 清洗部门名称
    df['bumen_new'] = df[department].map(lambda x :'电商平台部' if '电商平台部' in x else(x[1:] if 'Y' in x or ' W' in x else x))
    # list_bm = ['零售事业组','批发流通事业组','线下渠道部']
    df['bumen'] = df['bumen_new'].map(lambda x :'渠道' if '零售事业组' in x or '批发流通事业组' in x or '线下渠道部' in x else x)
    # 返回原表所属年度月度
    df['year'] = df['riqi'].map(lambda x: str(x)[:4])      
    df['month'] = df['riqi'].map(lambda x: str(x)[5:7])
    # 增加年平均采购成本
    df = pd.merge(df, dfc, left_on=['wuliaomc'], right_on=['wuliaomc'], how = 'left')# 'wlmc_new','wlmc_all'
    # 计算总成本
    df['purchases'] = df['cost'] * df[salename]
    # 计算利润
    df['profit'] = df['jiashuihj'] - df['purchases']
    # 转日期格式
    df['riqi'] = pd.to_datetime(df['riqi'].map(lambda x: str(x)[:10]), format='%Y-%m-%d')


    df['kehuid'] = df['kehuid'].map(lambda x:str(x).zfill(8))

    return df



# 判断上市阶段函数
# @df_key_product：产品上市时间表
# @df：主表
# @name：销量字段名
def func(df_key_product,df,name):

    # 客户销量分级标签
    df0 = df.groupby(['wuliaomc','kehumc'],as_index = False)[name].sum()
    df0['label'] =  df0[name].map(lambda x:'<=100' if x<= 100 else ('100-500' 
                                                   if x <= 500 else ('500-1000' 
                                                   if x<= 1000 else '>=1000')))
    df0.drop(labels=name, axis=1, inplace=True)                                         
    df = pd.merge(df,df0, on=['wuliaomc','kehumc'], how = 'left')

    df_key_product['riqi'] = df_key_product['riqi'].map(lambda x:str(x)[:10])
    df_key_product['riqi'] = pd.to_datetime(df_key_product['riqi'],format="%Y-%m-%d")

    # 增加上市日期
    df_key_product['month'] = df_key_product['riqi'].map(lambda x:x.month)
    df_key_product['year'] = df_key_product['riqi'].map(lambda x:x.year)
    df_key_product['state'] = '上市当月'
    df_key_product01 = df_key_product[['wlmc_all','year','month','state']]

    def a(num):
        list_ma = []
        list_ya = []
        for i in range(len(df_key_product01)):
            if df_key_product01['month'][i] + num > 12:
                list_ma.append(df_key_product01['month'][i] + num - 12)
                list_ya.append(df_key_product01['year'][i] + 1)
            else:
                list_ma.append(df_key_product01['month'][i] + num)
                list_ya.append(df_key_product01['year'][i])
        df_m = pd.DataFrame(list_ma, columns= ['month'])
        df_y = pd.DataFrame(list_ya, columns= ['year'])

        return df_m, df_y 

    df_important_product02 = pd.concat([df_key_product01['wlmc_all'], a(1)[1], a(1)[0]], axis=1)
    df_important_product03 = pd.concat([df_key_product01['wlmc_all'], a(2)[1], a(2)[0]], axis=1)
    df_important_product02['state'] = '上市第2个月'
    df_important_product03['state'] = '上市第3个月'
    # 判断上市月份
    df_important_productall = pd.concat([df_key_product01, df_important_product02, df_important_product03])
    df_important_productall['month'] = df_important_productall['month'].map(lambda x:str(x).zfill(2))
    df_important_productall['year'] = df_important_productall['year'].map(lambda x:str(x))
                                                                                                
    df = pd.merge(df, df_important_productall, left_on=['wuliaomc','month','year'], right_on=['wlmc_all','month','year'], how = 'left')
    df.drop(labels='wlmc_all', axis=1, inplace=True) 
    df['state'].fillna('上市第3个月后',inplace=True)

    return df


# 删仓函数
# @df：主表
# @name：日期字段
# @list_：需删的仓库id列表
def drop(df,name,list_):
    df[name] = pd.to_datetime(df[name], format='%Y-%m-%d')
    df1 = df[df[name]<datetime(2021,5,1)]
    df2 = df[df[name]>=datetime(2021,5,1)]
    df2 = df2[~df2['cangkuid'].isin(list_)]
    df = pd.concat([df1,df2],ignore_index=True)

    return df







# 增加省市
# @df：主表
def area(df):

    import numpy as np
    from sqlalchemy import create_engine,text

    engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format('root', '123456', 'localhost', '3306')) 

    township_area = pd.read_sql_query(text('select * from baidu_map.township_area;'), engine.connect()) 


    area1 = township_area[['name_prov1','name_city1']].drop_duplicates()
    dict_area1 = dict(zip(area1['name_city1'],area1['name_prov1']))
    # 地区表市区字典{市：[区]}
    area2 = township_area[['name_city1','name_coun1']].drop_duplicates()
    dict_area2 = dict(zip(area2['name_coun1'],area2['name_city1']))
    # 省列表
    list_pro = township_area['name_prov1'].drop_duplicates().to_list()
    # 市列表
    list_city = township_area['name_city1'].drop_duplicates().to_list()
    # 区列表
    list_coun = township_area['name_coun1'].drop_duplicates().to_list()

    def funca(columns_name):
        a = df[columns_name].drop_duplicates().dropna().reset_index()
        lista = []
        listb = []
        listc = []
        for j in a[columns_name]:
            try:
                lista.append([i for i in list_pro if i in j][0])
            except:
                lista.append(np.nan)
            try:
                listb.append([i for i in list_city if i in j][0])
            except:
                listb.append(np.nan)
            try:
                listc.append([i for i in list_coun if i in j][0])
            except:
                listc.append(np.nan)


        a['name_prov1'] = pd.DataFrame(lista)
        a['name_city1'] = pd.DataFrame(listb)
        a['name_coun1'] = pd.DataFrame(listc)

        a['name_city1'].fillna(a['name_coun1'],inplace=True)
        a['name_city1'].replace(dict_area2,inplace=True)
        a['name_prov1'].fillna(a['name_city1'],inplace=True)
        a['name_prov1'].replace(dict_area1,inplace=True)

        return a

    a = funca('shouhuofdz')
    b = funca('kehumc')


    c = df[['kehumc','shouhuofdz']].drop_duplicates()
    d = pd.merge(c,a.drop(['index'],axis=1),on=['shouhuofdz'],how='left')
    d1 = d[~d['name_prov1'].isna()]
    d2 = d[d['name_prov1'].isna()][['kehumc','shouhuofdz']]
    d2 = pd.merge(d2,b.drop(['index'],axis=1),on=['kehumc'],how='left')
    d = pd.concat([d1,d2],ignore_index=True)
    df = pd.merge(df,d,on=['kehumc','shouhuofdz'],how='left')

    area_1 = township_area[['name_prov1','name_prov']].drop_duplicates()
    area_2 = township_area[['name_city1','name_city']].drop_duplicates()
    area_3 = township_area[['name_coun1','name_coun']].drop_duplicates()
    df = pd.merge(df,area_1,on=['name_prov1'],how='left')
    df = pd.merge(df,area_2,on=['name_city1'],how='left')
    df = pd.merge(df,area_3,on=['name_coun1'],how='left')

    return df






# 客户加单分级
def level(df_saleship_return):
    # 一天算一次加单
    df_saleship_return['year'] = df_saleship_return['riqi'].map(lambda x:str(x)[:4])
    df_sales_num = df_saleship_return[['riqi','kehumc','year']].drop_duplicates()
    # 计算当年加单量                                              
    df_sales_level = df_sales_num.groupby(['year','kehumc'],as_index=False)['riqi'].count()
    # 按当年加单量计算分级                                                                           
    list_mean = []
    for i in range(len(df_sales_level)):
        if df_sales_level['year'][i] == str(datetime.now().year):
            mean = datetime.now().month/df_sales_level['riqi'][i]
            list_mean.append(mean)
        else:
            mean = 12/df_sales_level['riqi'][i]
            list_mean.append(mean)
    df_sales_level['mean'] = pd.DataFrame(list_mean)                                                                       
    # 增加分级 
    dict_level = {
        '<=1':[0,1],
        '1-2':[1,2],
        '2-3':[2,3],
        '3-6':[3,6]
    }
    df_sales_level['level'] = df_sales_level['mean'].map(lambda x :getDict(dict_level,x,'6-12'))
    df_sales_level = df_sales_level[['year','kehumc','level']]
    df_saleship_return = pd.merge(df_saleship_return,df_sales_level,on=['year','kehumc'],how='left')

    return df_saleship_return





# 表存入数据库
# @df：主表
# @database：数据库
# @table：存入表名
# @sql_create：sql建表语句
# @sql_insert：sql插入数据语句
def savesql(df,database,table,sql_create,sql_insert):

    from datetime import datetime
    import pymysql
    
    #保留非空值，以None空值的形式替换Nan空值
    df = df.fillna(0)

    db = pymysql.connect(host='localhost', user='root',password='123456', database=database)
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    cursor.execute("DROP TABLE IF EXISTS {}".format(table))
    # 使用预处理语句创建表

    cursor.execute(sql_create)
    # 批量创建数据
    userValues = [tuple(x) for x in df.values]

    # 记录执行前时间
    start_time = datetime.now()
    print("开始时间：", start_time)
    print("插入数据")
    try:
        # 执行sql语句
        cursor.executemany(sql_insert, userValues)
        # 执行sql语句
        db.commit()
    except:
        # 发生错误时回滚
        db.rollback()
        print(table+'插入失败')
    # 记录执行完成时间
    end_time = datetime.now()
    print("结束时间：", end_time)
    # 计算时间差
    time_d = end_time - start_time
    print(time_d)
    # 关闭数据库连接
    db.close()





def insertsql(df,database,table,sql_insert,date):

    from datetime import datetime
    import pymysql

    db = pymysql.connect(host='localhost', user='root',password='123456', database=database)
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    cursor.execute("""delete FROM {}.{}
                        where {}>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01');                   
                        """.format(database,table,date))
    db.commit()
    print(cursor.rowcount, "条数据已被删除")

    
    #保留非空值，以None空值的形式替换Nan空值
    df = df.fillna(0)

    # 批量创建数据
    userValues = [tuple(x) for x in df.values]

    # 记录执行前时间
    start_time = datetime.now()
    print("开始时间：", start_time)
    print("插入数据")
    try:
        # 执行sql语句
        cursor.executemany(sql_insert, userValues)
        # 执行sql语句
        db.commit()
    except:
        # 发生错误时回滚
        db.rollback()
        print(table+'插入失败')
    # 记录执行完成时间
    end_time = datetime.now()
    print("结束时间：", end_time)
    # 计算时间差
    time_d = end_time - start_time
    print(time_d)
    # 关闭数据库连接
    db.close()




def insertsql1(df,database,table,sql_insert):

    from datetime import datetime
    import pymysql

    db = pymysql.connect(host='localhost', user='root',password='123456', database=database)
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    cursor.execute("""delete FROM {}.{}
                        where shujuzx = '财务数据中心';
                        """.format(database,table))
    db.commit()
    print(cursor.rowcount, "条数据已被删除")

    
    #保留非空值，以None空值的形式替换Nan空值
    df = df.fillna(0)

    # 批量创建数据
    userValues = [tuple(x) for x in df.values]

    # 记录执行前时间
    start_time = datetime.now()
    print("开始时间：", start_time)
    print("插入数据")
    try:
        # 执行sql语句
        cursor.executemany(sql_insert, userValues)
        # 执行sql语句
        db.commit()
    except:
        # 发生错误时回滚
        db.rollback()
        print(table+'插入失败')
    # 记录执行完成时间
    end_time = datetime.now()
    print("结束时间：", end_time)
    # 计算时间差
    time_d = end_time - start_time
    print(time_d)
    # 关闭数据库连接
    db.close()














# 替换系统编码对应的新物料名称
# @df：主表
def cf(df):

    from sqlalchemy import create_engine,text
    engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format('root', '123456', 'localhost', '3306')) 

    df_classify = pd.read_sql_query(text('SELECT wuliaobm,wuliaomc FROM `erp_jd_dwd`.`erp_jd_dwd_fact_classify`;'),   engine.connect())  
    
    df1 = pd.merge(df[['wuliaobm','wuliaomc']].drop_duplicates(),df_classify[['wuliaobm','wuliaomc']],on=['wuliaobm'],how='left')
    df1.dropna(inplace=True)
    df1 = df1[df1['wuliaomc_x']!=df1['wuliaomc_y']]
    dictc = dict(zip(df1['wuliaomc_x'],df1['wuliaomc_y']))
    df['wuliaomc'].replace(dictc,inplace=True)
    return df


# 替换系统客户对应的新客户名称
# @df：主表
def ct(df):

    from sqlalchemy import create_engine,text
    engine = create_engine("mysql+pymysql://{}:{}@{}:{}".format('root', '123456', 'localhost', '3306'))  

    customer_name_change = pd.read_sql_query(text('select 客户原抬头,客户 from localdata.customer_name_change;'), engine.connect())

    customer_name_change['客户禁用抬头'] = customer_name_change['客户原抬头'].map(lambda x:x+'（禁用）')

    dicta = dict(zip(customer_name_change['客户原抬头'],customer_name_change['客户']))
    dictb = dict(zip(customer_name_change['客户禁用抬头'],customer_name_change['客户']))
    df['kehumc'].replace(dicta,inplace=True)
    df['kehumc'].replace(dictb,inplace=True)
    
    return df




# 运行sql文件
def sqlrun(path):
    
    import pymysql
    conn  = pymysql.connect(host="localhost",port = 3306, user="root", password="123456",charset="utf8")
    c = conn.cursor()

    with open(path, 'r', True, 'UTF-8') as f:
        sql = f.read()
        sql = sql.replace('\n' , ' ').replace('\t' , ' ')
        for i in sql.split(';'):
            try:
                c.execute(i)  
            except:
                pass 
    conn.commit()
    c.close()
    conn.close()