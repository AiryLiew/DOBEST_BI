# -*- coding: utf-8 -*-
# 测试环境: python3.9


def func(url,shujuzx,name1,name2,name3,name4,name5):
    import sys
    sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

    import json
    import pandas as pd
    from sqlalchemy import create_engine,text
    from api_request import api_request
    from datetime import datetime
    
    engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '123456', 'localhost', '3306', 'erp_jd_ods'))
    # conn = create_engine("mssql+pymssql://{}:{}@{}:{}/{}".format('sa', '123456', '10.242.32.59', '1433', 'erp_jd_ods'))

    method = "POST"
    headers = None

    def xclsh(jigoumc,xclsh):
        
        params = {			
            "shujuzx":shujuzx, 
            "jigoumc":jigoumc,   
            'xuchuanlsh':xclsh,
            'dancisjl':5000
        }
        result = api_request(method=method, url=url, params=params, headers=headers)
        body = result.text
        response = json.loads(body)
        xclsh0 = response["xclsh"]
        status_code = response["backdata"] # 返回数据
        df = pd.json_normalize(status_code) 
        try:
            list_ = []
            for i in range(len(df)):
                for j in df['zijianmxs'][i]:
                    j.update({'rukurq':df['rukurq'][i],'shiwulx':df['shiwulx'][i]+'子件','danjubh':df['danjubh'][i]})
                list_.append(pd.json_normalize(df['zijianmxs'][i]))
            df1 = pd.concat(list_,ignore_index=True)  
            df.drop(['zijianmxs'],axis=1,inplace=True) 
            df = pd.concat([df,df1],ignore_index=True) 
            return df, xclsh0  
        except:
            return df, xclsh0

    def funcA(company):
        df = pd.DataFrame()
        xclsh_start = 0
        while True:
            df_start, xclsh_start = xclsh(company,xclsh_start)
            df = df.append(df_start)
            if xclsh_start==-1:
                break
        return df
    try:
        df_wc = funcA("杭州游卡文化创意有限公司")
        if df_wc.empty==False:
            df_wc['company'] = "杭州游卡文化创意有限公司"
            df_wc['refresh_jk'] = datetime.now()
            df_wc.to_sql(name1, engine, schema='erp_jd_ods', if_exists='replace', index=False)

    except:
        print(name1 + ' have no data')

    try:
        df_yc = funcA("杭州泳淳网络技术有限公司")
        if df_yc.empty==False:
            df_yc['company'] = "杭州泳淳网络技术有限公司"
            df_yc['refresh_jk'] = datetime.now()
            df_yc.to_sql(name2, engine, schema='erp_jd_ods', if_exists='replace', index=False)

    except:
        print(name2 + ' have no data')

    try:
        df_ms = funcA("杭州迷思文化创意有限公司")
        if df_ms.empty==False:
            df_ms['company'] = "杭州迷思文化创意有限公司"
            df_ms['refresh_jk'] = datetime.now()
            df_ms.to_sql(name3, engine, schema='erp_jd_ods', if_exists='replace', index=False)

    except:
        print(name3 + ' have no data')
    
    try:
        df_kyk = funcA("上海卡丫卡文化传播有限公司")
        if df_kyk.empty==False:
            df_kyk['company'] = "上海卡丫卡文化传播有限公司"
            df_kyk['refresh_jk'] = datetime.now()
            df_kyk.to_sql(name4, engine, schema='erp_jd_ods', if_exists='replace', index=False)

    except:
        print(name4 + ' have no data')



    try:
        df_kyok = funcA("上海卡哟卡网络技术有限公司")
        if df_kyok.empty==False:
            df_kyok['company'] = "上海卡哟卡网络技术有限公司"
            df_kyok['refresh_jk'] = datetime.now()
            df_kyok.to_sql(name5, engine, schema='erp_jd_ods', if_exists='replace', index=False)

    except:
        print(name5 + ' have no data')

   
    engine.dispose() # 关闭连接
    print(datetime.now())



# 其他出库
def func_QTCK(shujuzx,name1,name2,name3,name4,name5):
    import sys
    sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

    import json
    import pandas as pd
    from sqlalchemy import create_engine,text
    from api_request import api_request
    from datetime import datetime
    
    engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '123456', 'localhost', '3306', 'erp_jd_ods'))
    # conn = create_engine("mssql+pymssql://{}:{}@{}:{}/{}".format('sa', '123456', '10.242.21.1', '1433', 'erp_jd_ods'))

    method = "POST"
    headers = None

    def funcA(shujuzx,jigoumc):
        params = {			
                    "shujuzx":shujuzx, 
                    "jigoumc":jigoumc,   

                }
        result = api_request(method=method, url="http://10.225.137.124:7772/ZyyxDSS/GetQiTaCKD", params=params, headers=headers)
        body = result.text
        response = json.loads(body)
        status_code = response["backdata"] # 返回数据
        df = pd.json_normalize(status_code) 
        return df


    try:
        df_wc = funcA(shujuzx,"杭州游卡文化创意有限公司")
        if df_wc.empty==False:
            df_wc['company'] = "杭州游卡文化创意有限公司"
            df_wc['refresh_jk'] = datetime.now()
            df_wc.to_sql(name1, engine, schema='erp_jd_ods', if_exists='replace', index=False)

    except:
        print(name1 + ' have no data')


    try:
        df_yc = funcA(shujuzx,"杭州泳淳网络技术有限公司")
        if df_yc.empty==False:
            df_yc['company'] = "杭州泳淳网络技术有限公司"
            df_yc['refresh_jk'] = datetime.now()
            df_yc.to_sql(name2, engine, schema='erp_jd_ods', if_exists='replace', index=False)

    except:
        print(name2 + ' have no data')


    try:
        df_ms = funcA(shujuzx,"杭州迷思文化创意有限公司")
        if df_ms.empty==False:
            df_ms['company'] = "杭州迷思文化创意有限公司"
            df_ms['refresh_jk'] = datetime.now()
            df_ms.to_sql(name3, engine, schema='erp_jd_ods', if_exists='replace', index=False)

    except:
        print(name3 + ' have no data')

   
    try:
        df_kyk = funcA(shujuzx,"上海卡丫卡文化传播有限公司")
        if df_kyk.empty==False:
            df_kyk['company'] = "上海卡丫卡文化传播有限公司"
            df_kyk['refresh_jk'] = datetime.now()
            df_kyk.to_sql(name4, engine, schema='erp_jd_ods', if_exists='replace', index=False)

    except:
        print(name4 + ' have no data')


    try:
        df_kyok = funcA(shujuzx,"上海卡哟卡网络技术有限公司")
        if df_kyok.empty==False:
            df_kyok['company'] = "上海卡哟卡网络技术有限公司"
            df_kyok['refresh_jk'] = datetime.now()
            df_kyok.to_sql(name5, engine, schema='erp_jd_ods', if_exists='replace', index=False)

    except:
        print(name5 + ' have no data')

    engine.dispose() # 关闭连接
    print(datetime.now())



# 无机构名称数据
def func_wjg(url,shujuzx,name):
    import sys
    sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

    import json
    import pandas as pd
    from sqlalchemy import create_engine,text
    from api_request import api_request
    from datetime import datetime
    
    engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '123456', 'localhost', '3306', 'erp_jd_ods'))
    method = "POST"
    headers = None

    def xclsh(xclsh):
        
        params = {			
            "shujuzx":shujuzx,   
            'xuchuanlsh':xclsh,
            'dancisjl':5000
        }
        result = api_request(method=method, url=url, params=params, headers=headers)
        body = result.text
        response = json.loads(body)
        xclsh0 = response["xclsh"]
        status_code = response["backdata"] # 返回数据
        df = pd.json_normalize(status_code) 
        try:
            list_ = []
            for i in range(len(df)):
                for j in df['zijianmxs'][i]:
                    j.update({'rukurq':df['rukurq'][i],'shiwulx':df['shiwulx'][i]+'子件','danjubh':df['danjubh'][i]})
                list_.append(pd.json_normalize(df['zijianmxs'][i]))
            df1 = pd.concat(list_,ignore_index=True)  
            df.drop(['zijianmxs'],axis=1,inplace=True) 
            df = pd.concat([df,df1],ignore_index=True) 
            return df, xclsh0  
        except:
            return df, xclsh0

    def funcA():
        df = pd.DataFrame()
        xclsh_start = 0
        while True:
            df_start, xclsh_start = xclsh(xclsh_start)
            df = df.append(df_start)
            if xclsh_start==-1:
                break
        return df
    try:
        df_wc = funcA()
        if df_wc.empty==False:
            df_wc['refresh_jk'] = datetime.now()
            df_wc.to_sql(name, engine, schema='erp_jd_ods', if_exists='replace', index=False)

    except:
        print(name + ' have no data')

   
    engine.dispose() # 关闭连接
    print(datetime.now())









# 仅有数据中心
def func_sjzx(url,shujuzx,name):
    import sys
    sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

    import json
    import pandas as pd
    from sqlalchemy import create_engine,text
    from api_request import api_request
    from datetime import datetime
    
    engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '123456', 'localhost', '3306', 'erp_jd_ods'))
    method = "POST"
    headers = None


        
    params = {			
            "shujuzx":shujuzx
        }
    result = api_request(method=method, url=url, params=params, headers=headers)
    body = result.text
    response = json.loads(body)
    status_code = response["backdata"] # 返回数据
    df = pd.json_normalize(status_code) 


    if df.empty==False:
        df['refresh_jk'] = datetime.now()
        df.to_sql(name, engine, schema='erp_jd_ods', if_exists='replace', index=False)


   
    engine.dispose() # 关闭连接
    print(datetime.now())