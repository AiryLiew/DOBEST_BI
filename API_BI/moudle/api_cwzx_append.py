# -*- coding: utf-8 -*-
# 测试环境: python3.10.1


# 初始抽取接口数据函数
def s_func(url,company,name):
    import sys
    sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

    import json
    import pandas as pd
    from api_request import api_request


    method = "POST"
    headers = None


    dcsjl = 400

    if name == 'erp_jd_ods_dim_saleshipping_wc_cwzx':
        list_1 = [459788]
    else:
        list_1 = [0]
    # list_1 = [0]
    
    list_2 = []
    data = pd.DataFrame()
    while True:
        xclsh = list_1[-1]
        if xclsh!=-1:
            params = {			
                            "shujuzx":"财务数据中心", 
                            "jigoumc":company,   
                            'xuchuanlsh':xclsh,
                            'dancisjl':dcsjl
                        }
            result = api_request(method=method, url=url, params=params, headers=headers)
            body = result.text
            response = json.loads(body)
            xclsh = response["xclsh"]
            list_1.append(xclsh)
            status_code = response["backdata"] # 返回数据
            df = pd.json_normalize(status_code)
            len_= len(df)

            try:
                list_ = []
                for i in range(len_):
                    for j in df['zijianmxs'][i]:
                        j.update({'rukurq':df['rukurq'][i],'shiwulx':df['shiwulx'][i]+'子件','danjubh':df['danjubh'][i]})
                    list_.append(pd.json_normalize(df['zijianmxs'][i]))
                df1 = pd.concat(list_,ignore_index=True)  
                df.drop(['zijianmxs'],axis=1,inplace=True) 
                df = pd.concat([df,df1],ignore_index=True) 
                list_2.append(df)
            except:
                list_2.append(df)
            
            data = pd.concat(list_2,ignore_index=True)
             
        else:
            break


        
    return data


def s_funcB(url,company,name): 

    from sqlalchemy import create_engine,text
    from datetime import datetime
        
    engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '123456', 'localhost', '3306', 'erp_jd_ods'))

    try:
        dfc = s_func(url,company,name)
        if dfc.empty==False:
            dfc['company'] = company
            dfc['refresh_jk'] = datetime.now()
            dfc.to_sql(name, engine, schema='erp_jd_ods', if_exists='replace', index=False)

            print(datetime.now())

   

    except:
        print(name + ' have no data') 

    engine.dispose() 






def a_func(url,company,name,fid):
    import sys
    sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

    import json
    import pandas as pd
    from api_request import api_request
    from datetime import datetime

    from sqlalchemy import create_engine,text
        
    engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '123456', 'localhost', '3306', 'erp_jd_ods'))

    # 删除本月数据后，取数据库中最新流水号
    def func_fid(table,fid):
        df = pd.read_sql_query(text('SELECT {} FROM {}\
                                order by {} desc LIMIT 1;'.format(fid,table,fid)), engine.connect())
        return df[fid].values[0]
    try:
        xclsh = func_fid(name,fid)


        list_2 = []
        method = "POST"
        headers = None
        while xclsh!=-1:

            params = {			
                        "shujuzx":"财务数据中心", 
                        "jigoumc":company,   
                        'xuchuanlsh':xclsh,
                        'dancisjl':500
                    }
            result = api_request(method=method, url=url, params=params, headers=headers)
            body = result.text
            response = json.loads(body)
            xclsh = response["xclsh"] 
            print(xclsh)
            status_code = response["backdata"] # 返回数据
            df = pd.json_normalize(status_code)
            len_ = len(df)
            try:
                list_ = []
                for i in range(len_):
                    for j in df['zijianmxs'][i]:
                        j.update({'rukurq':df['rukurq'][i],'shiwulx':df['shiwulx'][i]+'子件','danjubh':df['danjubh'][i]})
                    list_.append(pd.json_normalize(df['zijianmxs'][i]))
                df1 = pd.concat(list_,ignore_index=True)  
                df.drop(['zijianmxs'],axis=1,inplace=True) 
                df = pd.concat([df,df1],ignore_index=True) 
                list_2.append(df)
            except:
                list_2.append(df)
                
            data = pd.concat(list_2,ignore_index=True)

        try:
            if data.empty==False:
                data['company'] = company
                data['refresh_jk'] = datetime.now()
                data.to_sql(name, engine, schema='erp_jd_ods', if_exists='append', index=False)
                print(datetime.now())

    

        except:
            print(name + ' have no data')  


        engine.dispose()
        return data
        
    except:
        print(name + '  not exists')

    






















# 无机构接口
# 初始抽取接口数据函数
def s1_func(url):
    import sys
    sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

    import json
    import pandas as pd
    from api_request import api_request


    method = "POST"
    headers = None


    dcsjl = 5000
    list_1 = [0]
    list_2 = []
    data = pd.DataFrame()
    while True:
        xclsh = list_1[-1]
        if xclsh!=-1:
            params = {			
                            "shujuzx":"财务数据中心", 
                            'xuchuanlsh':xclsh,
                            'dancisjl':dcsjl
                        }
            result = api_request(method=method, url=url, params=params, headers=headers)
            body = result.text
            response = json.loads(body)
            xclsh = response["xclsh"]
            list_1.append(xclsh)
            status_code = response["backdata"] # 返回数据
            df = pd.json_normalize(status_code)
            len_= len(df)

            try:
                list_ = []
                for i in range(len_):
                    for j in df['zijianmxs'][i]:
                        j.update({'rukurq':df['rukurq'][i],'shiwulx':df['shiwulx'][i]+'子件','danjubh':df['danjubh'][i]})
                    list_.append(pd.json_normalize(df['zijianmxs'][i]))
                df1 = pd.concat(list_,ignore_index=True)  
                df.drop(['zijianmxs'],axis=1,inplace=True) 
                df = pd.concat([df,df1],ignore_index=True) 
                list_2.append(df)
            except:
                list_2.append(df)
            
            data = pd.concat(list_2,ignore_index=True)
             
        else:
            break

    if len_ >0 and len_ <= dcsjl :
        params = {
                        "shujuzx":"财务数据中心", 
                        'xuchuanlsh':list_1[-2],
                        'dancisjl':len_}
        result = api_request(method=method, url=url, params=params, headers=headers)
        body = result.text
        response = json.loads(body)
        xclsh = response["xclsh"]
        list_1=[xclsh if i ==-1 else i for i in list_1]

        
    return data


def s1_funcB(url,name): 

    from sqlalchemy import create_engine,text
    from datetime import datetime
        
    engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '123456', 'localhost', '3306', 'erp_jd_ods'))
    try:
        dfc = s1_func(url)
        if dfc.empty==False:
            dfc['refresh_jk'] = datetime.now()
            dfc.to_sql(name, engine, schema='erp_jd_ods', if_exists='replace', index=False)
            print(datetime.now())
   

    except:
        print(name + ' have no data') 


    engine.dispose() 











def a1_func(url,name,fid):
    import sys
    sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

    import json
    import pandas as pd
    from api_request import api_request
    from datetime import datetime

    from sqlalchemy import create_engine,text
        
    engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '123456', 'localhost', '3306', 'erp_jd_ods'))

    # 取数据库中最新流水号
    def func_fid(table,fid):
        df = pd.read_sql_query(text('SELECT {} FROM {}\
                                order by {} desc LIMIT 1;'.format(fid,table,fid)), engine.connect())
        return df[fid].values[0]
    try:
        xclsh = func_fid(name,fid)


        list_2 = []
        method = "POST"
        headers = None
        while xclsh!=-1:

            params = {			
                        "shujuzx":"财务数据中心",  
                        'xuchuanlsh':xclsh,
                        'dancisjl':500
                    }
            result = api_request(method=method, url=url, params=params, headers=headers)
            body = result.text
            response = json.loads(body)
            xclsh = response["xclsh"] 
            print(xclsh)
            status_code = response["backdata"] # 返回数据
            df = pd.json_normalize(status_code)
            len_ = len(df)
            try:
                list_ = []
                for i in range(len_):
                    for j in df['zijianmxs'][i]:
                        j.update({'rukurq':df['rukurq'][i],'shiwulx':df['shiwulx'][i]+'子件','danjubh':df['danjubh'][i]})
                    list_.append(pd.json_normalize(df['zijianmxs'][i]))
                df1 = pd.concat(list_,ignore_index=True)  
                df.drop(['zijianmxs'],axis=1,inplace=True) 
                df = pd.concat([df,df1],ignore_index=True) 
                list_2.append(df)
            except:
                list_2.append(df)
                
            data = pd.concat(list_2,ignore_index=True)

        try:
            if data.empty==False:
                data['refresh_jk'] = datetime.now()
                data.to_sql(name, engine, schema='erp_jd_ods', if_exists='append', index=False)
                print(datetime.now())

    

        except:
            print(name + ' have no data')  

        engine.dispose()
        return data
        
    except:
        print(name + '  not exists')










# 一次返回数据
def onceback(url,name):
    import sys
    sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

    import json
    import pandas as pd
    from api_request import api_request
    from sqlalchemy import create_engine,text
    from datetime import datetime
        
    engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '123456', 'localhost', '3306', 'erp_jd_ods'))


    method = "POST"
    headers = None
    params = {			
            "shujuzx":"财务数据中心"
            }
    result = api_request(method=method, url=url, params=params, headers=headers)
    body = result.text
    response = json.loads(body)
    backdata = response['backdata']
    df = pd.json_normalize(backdata)


    df['refresh_jk'] = datetime.now()
    df.to_sql(name, engine, schema='erp_jd_ods', if_exists='replace', index=False)

    print(datetime.now())

    return df