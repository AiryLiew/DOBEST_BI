# -*- coding: utf-8 -*-
# 测试环境: python3.9.6


# *****************************************导入库********************************************************#
from __future__ import division
from sys import exit
from math import sqrt
from numpy import array
from scipy.optimize import fmin_l_bfgs_b
import pandas as pd 
import matplotlib.pyplot as plt
from sqlalchemy import create_engine,text
# 显示汉字
plt.rc("font",family='FangSong')
from io import BytesIO
import base64
from datetime import datetime


# *****************************************取数据********************************************************#
engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '123456', 'localhost', '3306', 'erp_jd_dwd')) 
engine1 = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '123456', 'localhost', '3306', 'bi')) 
conn = create_engine("mssql+pymssql://{}:{}@{}:{}/{}".format('sa', '123456', '10.242.21.1', '1433', 'bi'))

df_sales = pd.read_sql_query(text('select * from erp_jd_dwd_dim_saleshipping;'), engine.connect())


# ******************************************数据清洗*****************************************************#
# 去除内部订单
list_ = ['杭州游卡文化创意有限公司','上海卡丫卡文化传播有限公司','杭州泳淳网络技术有限公司','杭州迷思文化创意有限公司']
df = df_sales[~df_sales['kehumc'].isin(list_)]
df['y_m'] = df['year'] +  df['month']
df = df.groupby('y_m')['shifasl'].sum()


# ********************************************自定义函数*************************************************#

# Y：   实际值
# a：   平滑值
# b:    趋势性
# s:    季节性趋势
# m:    预测时间
def RMSE(params, *args):#位置参数

    Y = args[0]
    type = args[1]
    rmse = 0

    if type == 'linear':
        #线性回归，两参
        #初始值
        alpha, beta = params
        a = [Y[0]]
        b = [Y[1] - Y[0]]
        y = [a[0] + b[0]]

        for i in range(len(Y)):

            a.append(alpha * Y[i] + (1 - alpha) * (a[i] + b[i]))
            b.append(beta * (a[i + 1] - a[i]) + (1 - beta) * b[i])
            y.append(a[i + 1] + b[i + 1])

    else:
        #三参
        alpha, beta, gamma = params
        m = args[2]     
        a = [sum(Y[0:m]) / float(m)]
        b = [(sum(Y[m:2 * m]) - sum(Y[0:m])) / m ** 2]

        if type == 'additive':
            #累加性
            s = [Y[i] - a[0] for i in range(m)]
            y = [a[0] + b[0] + s[0]]

            for i in range(len(Y)):

                a.append(alpha * (Y[i] - s[i]) + (1 - alpha) * (a[i] + b[i]))
                b.append(beta * (a[i + 1] - a[i]) + (1 - beta) * b[i])
                s.append(gamma * (Y[i] - a[i] - b[i]) + (1 - gamma) * s[i])
                y.append(a[i + 1] + b[i + 1] + s[i + 1])

        elif type == 'multiplicative':
            #累乘性
            s = [Y[i] / a[0] for i in range(m)]
            y = [(a[0] + b[0]) * s[0]]

            for i in range(len(Y)):

                a.append(alpha * (Y[i] / s[i]) + (1 - alpha) * (a[i] + b[i]))
                b.append(beta * (a[i + 1] - a[i]) + (1 - beta) * b[i])
                s.append(gamma * (Y[i] / (a[i] + b[i])) + (1 - gamma) * s[i])
                y.append((a[i + 1] + b[i + 1]) * s[i + 1])

        else:

            exit('Type must be either linear, additive or multiplicative')

    rmse = sqrt(sum([(m - n) ** 2 for m, n in zip(Y, y[:-1])]) / len(Y))

    return rmse


# 两参
def linear(x, fc, alpha = None, beta = None):

    Y = x[:]

    if (alpha == None or beta == None):

        initial_values = array([0.3, 0.1])
        boundaries = [(0, 1), (0, 1)]
        type = 'linear'

        parameters = fmin_l_bfgs_b(RMSE, x0 = initial_values, args = (Y, type), bounds = boundaries, approx_grad = True)
        alpha, beta = parameters[0]

    a = [Y[0]]
    b = [Y[1] - Y[0]]
    y = [a[0] + b[0]]
    rmse = 0

    for i in range(len(Y) + fc):

        if i == len(Y):
            Y.append(a[-1] + b[-1])

        a.append(alpha * Y[i] + (1 - alpha) * (a[i] + b[i]))
        b.append(beta * (a[i + 1] - a[i]) + (1 - beta) * b[i])
        y.append(a[i + 1] + b[i + 1])

    rmse = sqrt(sum([(m - n) ** 2 for m, n in zip(Y[:-fc], y[:-fc - 1])]) / len(Y[:-fc]))

    return Y[-fc:], alpha, beta, rmse


def plot_linear(x, fc,a): 
    lin = data[-1:] + linear(x, fc)[0]
    lin = pd.DataFrame(lin, columns=['预测值'])
    lin.index = lin.index + len(data)-1
    data1 = pd.DataFrame(data, columns=['实际值'])
    plt.figure(figsize=(26,8))
    plt.plot(data1.index, data1['实际值'], label = '实际值')
    plt.plot(lin.index, lin['预测值'], label = '预测值')
    plt.legend(loc='best')
    plt.title(a+'linear')
    # plt.show()
    # 转成图片的步骤
    sio = BytesIO()
    plt.savefig(sio, format='png')
    d = base64.encodebytes(sio.getvalue()).decode()
    html = '''
       <html>
           <body>
               <img src="data:image/png;base64,{}" />
           </body>
        <html>
    '''
    # plt.close()
    # 记得关闭，不然画出来的图是重复的
    return html.format(d)
    #format的作用是将data填入{}


# RMSE : callable f(x,*args)   最小化的目标，一般是loss函数
# x0 : ndarray                 最初的猜测，即待更新参数初始值。
# fprime :                     梯度函数，本函数未设
# args :                       上RMSE函数的参数
# bounds : list                (min, max) pairs for each element in x, defining the bounds on that parameter. 
# approx_grad                  返回数字近似梯度

def multiplicative(x, m, fc, alpha = None, beta = None, gamma = None):

    Y = x[:]

    if (alpha == None or beta == None or gamma == None):

        initial_values = array([0.0, 1.0, 0.0])
        boundaries = [(0, 1), (0, 1), (0, 1)]
        type = 'multiplicative'

        parameters = fmin_l_bfgs_b(RMSE, x0 = initial_values, args = (Y, type, m), bounds = boundaries, approx_grad = True)
        alpha, beta, gamma = parameters[0]
    # 初始值 a表示baseline， b表示趋势，s表示季节性，y表示预测值，m表示周期，分别取第一个周期的统计数据为初始值 
    a = [sum(Y[0:m]) / float(m)]
    b = [(sum(Y[m:2 * m]) - sum(Y[0:m])) / m ** 2]
    s = [Y[i] / a[0] for i in range(m)]
    y = [(a[0] + b[0]) * s[0]]
    rmse = 0
    # 套用上面公式，从0开始，fc表示预测的数量，如已知前7天，预测接下来的一个小时的数据，如果数据粒度是5分钟，fc为12。
    for i in range(len(Y) + fc):

        if i == len(Y):
            Y.append((a[-1] + b[-1]) * s[-m])
        # 预测值为
        a.append(alpha * (Y[i] / s[i]) + (1 - alpha) * (a[i] + b[i]))
        b.append(beta * (a[i + 1] - a[i]) + (1 - beta) * b[i])
        s.append(gamma * (Y[i] / (a[i] + b[i])) + (1 - gamma) * s[i])
        y.append((a[i + 1] + b[i + 1]) * s[i + 1])
    # 计算rmse值 
    rmse = sqrt(sum([(m - n) ** 2 for m, n in zip(Y[:-fc], y[:-fc - 1])]) / len(Y[:-fc]))

    return Y[-fc:], alpha, beta, gamma, rmse


def plot_mul(x, m, fc,a): 
    mul = data[-1:] + multiplicative(x, m, fc)[0]
    mul = pd.DataFrame(mul, columns=['预测值'])
    mul.index = mul.index + len(data)-1
    data1 = pd.DataFrame(data, columns=['实际值'])
    plt.figure(figsize=(26,8))
    plt.plot(data1.index, data1['实际值'], label = '实际值')
    plt.plot(mul.index, mul['预测值'], label = '预测值')
    plt.legend(loc='best')
    plt.title(a+'mul')
    # plt.show()
    # 转成图片的步骤
    sio = BytesIO()
    plt.savefig(sio, format='png')
    d = base64.encodebytes(sio.getvalue()).decode()
    html = '''
       <html>
           <body>
               <img src="data:image/png;base64,{}" />
           </body>
        <html>
    '''
    # plt.close()
    # 记得关闭，不然画出来的图是重复的
    return html.format(d)
    #format的作用是将data填入{}


def additive(x, m, fc, alpha = None, beta = None, gamma = None):

    Y = x[:]

    if (alpha == None or beta == None or gamma == None):

        initial_values = array([0.3, 0.1, 0.1])
        boundaries = [(0, 1), (0, 1), (0, 1)]
        type = 'additive'

        parameters = fmin_l_bfgs_b(RMSE, x0 = initial_values, args = (Y, type, m), bounds = boundaries, approx_grad = True)
        alpha, beta, gamma = parameters[0]

    a = [sum(Y[0:m]) / float(m)]
    b = [(sum(Y[m:2 * m]) - sum(Y[0:m])) / m ** 2]
    s = [Y[i] - a[0] for i in range(m)]
    y = [a[0] + b[0] + s[0]]
    rmse = 0

    for i in range(len(Y) + fc):

        if i == len(Y):
            Y.append(a[-1] + b[-1] + s[-m])

        a.append(alpha * (Y[i] - s[i]) + (1 - alpha) * (a[i] + b[i]))
        b.append(beta * (a[i + 1] - a[i]) + (1 - beta) * b[i])
        s.append(gamma * (Y[i] - a[i] - b[i]) + (1 - gamma) * s[i])
        y.append(a[i + 1] + b[i + 1] + s[i + 1])
    #最小二乘法计算误差（L-BFGS)
    rmse = sqrt(sum([(m - n) ** 2 for m, n in zip(Y[:-fc], y[:-fc - 1])]) / len(Y[:-fc]))

    return Y[-fc:], alpha, beta, gamma, rmse


def plot_add(x, m, fc, a):
    add = data[-1:] + additive(x, m, fc)[0]
    add = pd.DataFrame(add, columns=['预测值'])
    add.index = add.index + len(data)-1
    data1 = pd.DataFrame(data, columns=['实际值'])
    plt.figure(figsize=(26,8))
    plt.plot(data1.index, data1['实际值'], label = '实际值')
    plt.plot(add.index, add['预测值'], label = '预测值')
    plt.legend(loc='best')
    plt.title(a+'add')
    # plt.show()
    # 转成图片的步骤
    sio = BytesIO()
    plt.savefig(sio, format='png')
    d = base64.encodebytes(sio.getvalue()).decode()
    html = '''
       <html>
           <body>
               <img src="data:image/png;base64,{}" />
           </body>
        <html>
    '''
    # plt.close()
    # 记得关闭，不然画出来的图是重复的
    return html.format(d)
    #format的作用是将data填入{}


# *****************************************调用函数******************************************************#
# 预测月数
a = 12 - datetime.now().month + 1

data = df.values.tolist()[:-1]
chart = plot_mul(data, 12, a, str(df.index))
Y = multiplicative(data, 12, a)[0]
# rmse = multiplicative(data, 12, 12)[4]
# Y.append(rmse)

chart = plot_add(data, 12, a, str(df.index))
Y2 = additive(data,12, a)[0]
# rmse2 = additive(data, 12, 12)[4]
# Y2.append(rmse2)

def dataf(Y):
    yhat = [*data, *Y]
    list_index = []
    for j in range(2019,2023):
        for i in range(1,13):
            list_index.append(j*100+i)
    c = {'时间' : list_index,
         '销量' : yhat}
    df_predict = pd.DataFrame(c)
    
    now = datetime.now().year*100 + datetime.now().month
    df_predict['类型'] = df_predict['时间'].map(lambda x:'真实值' if x < now else '预测值')
    Index = df_predict[df_predict['类型']=='真实值'].index[-1]
    row = df_predict.loc[Index:Index].copy()
    row['类型'] = '预测值'
    df_predict = pd.concat([df_predict,row])

    return df_predict

df_predictmul = dataf(Y)
df_predictadd = dataf(Y2)


# *****************************************写入mysql*****************************************************#
df_predictmul.to_sql('bi_predictmul',engine1, schema='bi', if_exists='replace',index=False)
df_predictadd.to_sql('bi_predictadd',engine1, schema='bi', if_exists='replace',index=False)
engine.dispose()
engine1.dispose()


# *****************************************写入sql server************************************************#
df_predictmul.to_sql(name='bi_predictmul', con=conn, if_exists='replace',index=False)
df_predictadd.to_sql(name='bi_predictadd', con=conn, if_exists='replace',index=False)
conn.dispose() 