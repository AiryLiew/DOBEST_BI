def api_request(method, url, params=None, headers=None):

    import requests
    
    '''
    转发请求到目的主机
    @param method str 请求方法
    @param url str 请求地址
    @param params dict 请求参数
    @param headers dict 请求头
    '''
    method = str.upper(method)
    if method == 'POST':
        return requests.post(url=url, data=params, headers=headers)
    elif method == 'GET':
        return requests.get(url=url, params=params, headers=headers)
    else:
        return None


def mysql(table,database):#定义一个函数用来专门从数据库中读取数据

    from sqlalchemy import create_engine,text
    import pandas as pd

    host='localhost'
    user='root'
    password='123456'
    port=3306
    conn=create_engine('mysql+pymysql://{}:{}@{}:{}/{}'.format(user,password,host,port,database))
    sql=text('select * from '+str(table))
    results=pd.read_sql_query(sql,conn.connect())
    return results


def conn(database):#定义一个函数用来专门从数据库中读取数据

    from sqlalchemy import create_engine,text
    import pandas as pd

    host='localhost'
    user='root'
    password='123456'
    port=3306
    return create_engine('mysql+pymysql://{}:{}@{}:{}/{}'.format(user,password,host,port,database))
    