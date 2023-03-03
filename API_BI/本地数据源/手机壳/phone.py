import pandas as pd
import os
from sqlalchemy import create_engine


# *****************************************连接mysql、sql server*****************************************#
engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format('root', '123456', 'localhost', '3306', 'bi'))
conn = create_engine("mssql+pymssql://{}:{}@{}:{}/{}".format('sa', '123456', '10.242.21.1', '1433', 'bi'))


#设置文件夹路径，获取文件夹下的所有文件名
path_os =r'C:\Users\liujin02\Desktop\BI建设\API_BI\本地数据源\手机壳\数据文件'
files_list = os.listdir(path_os)


path =r'C:\Users\liujin02\Desktop\BI建设\API_BI\本地数据源\手机壳\数据2021.8.31.xlsx'
df1 = pd.read_excel(path,sheet_name='Sheet1')
df2 = pd.read_excel(path,sheet_name='Sheet2')


dfb = []
dfc = []
for file in files_list:
    file_path = path_os + "\\" + file


    def read_excel1(path):
        data_xlsx = pd.ExcelFile(path)
        list_sheet = data_xlsx.sheet_names[:-1]
        data = pd.DataFrame()
        for name in list_sheet:
            df = data_xlsx.parse(sheet_name=name)
            df.insert(0, "工作表", name)
            data = data.append(df)
        return data

    df = read_excel1(file_path)
    df = df.reset_index(drop=True)
    list_date = []
    for i in range(len(df)):
        df['工作表'][i] = df['工作表'][i].replace('日报','')


    df['工作表'] = df['工作表'].str.strip()# 去空格
    df['年'] = df['工作表'].map(lambda x: x.split('年')[0])
    snap0 = df['工作表'].map(lambda x: x.split('年')[1])
    df['月'] = snap0.map(lambda x: x.split('.')[0])
    df['日'] = snap0.map(lambda x: x.split('.')[1])
    df['日'] = df['日'].str.strip()# 去空格


    list_date = []
    a = '-'
    for i in range(len(df)):
        date = a.join([str(df['年'][i]),str(df['月'][i]),str(df['日'][i])])
        list_date.append(date)
    df['日期'] = pd.DataFrame(list_date)
    df['日期'] = pd.to_datetime(df['日期'], format = '%Y-%m-%d')
    df = df.reset_index(drop=True)
    df_snap_a = df['第四部分'].astype(str)

    result_b = []
    key_list_b = ['S', '其他']
    for i in range(len(df)):
        if any(key in df_snap_a[i] for key in key_list_b):
            new_b = pd.DataFrame(data={'日期':df['日期'][i],'皮肤名称':df['第四部分'][i], '皮肤类别':df['Unnamed: 2'][i], '销售数量':df['手机壳定制'][i]},index=[df.shape[0]])
            result_b.append(new_b)
    df_b = pd.concat(result_b, sort=False)
    df_b.sort_values(['日期'], inplace=True, ascending=True)

    result_c1 = []
    key_list_c = ['成本', '销售额']
    df_snap_c = df['Unnamed: 0'].astype(str)
    for i in range(len(df)):
        if any(key in df_snap_c[i] for key in key_list_c):
            new_c1 = pd.DataFrame(data={'日期':df['日期'][i],'类别':df['Unnamed: 0'][i], '数值':df['第四部分'][i]},index=[df.shape[0]])
            result_c1.append(new_c1)
    df_c = pd.concat(result_c1, sort=False)
    df_c['类别'] = df_c['类别'].map(lambda x: x.split('/')[0])
    df_c['数值'] = df_c['数值'].astype(float)
    df_c.sort_values(['日期'], inplace=True, ascending=True)
    dfb.append(df_b)
    dfc.append(df_c)

df_b = pd.concat(dfb, sort=False)
df_c = pd.concat(dfc, sort=False)


df_bb = df_b.groupby('日期',as_index=False)['销售数量'].sum()
df_bb['类别'] = '销量'
df_bb.rename(columns ={'销售数量':'数值'},inplace=True)
df_cc = pd.concat([df_c,df_bb])
df_c = df_cc.groupby(['日期','类别'])['数值'].sum().unstack().reset_index()


df_mx = pd.concat([df1,df_b])
df_tj = pd.concat([df2,df_c])


# *****************************************写入mysql*****************************************************#
df_mx.to_sql('bi_phonemx', engine, schema='bi', if_exists='replace', index=False)
df_tj.to_sql('bi_phonetj', engine, schema='bi', if_exists='replace', index=False)
engine.dispose()


# *****************************************写入sql server************************************************#
df_mx.to_sql(name='bi_phonemx', con=conn, if_exists='replace', index=False)
df_tj.to_sql(name='bi_phonetj', con=conn, if_exists='replace', index=False)
conn.dispose()  