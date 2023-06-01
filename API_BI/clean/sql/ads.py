# -*- coding: utf-8 -*-
# 测试环境: python3.9.6

# day refresh 19:30:00
import pymysql

conn  = pymysql.connect(host="localhost",port = 3306, user="root", password="123456",charset="utf8")
c = conn.cursor()


def sqlrun(path):
    with open(path, 'r', True, 'UTF-8') as f:
        sql = f.read()
        sql = sql.replace('\n' , ' ').replace('\t' , ' ')
        for i in sql.split(';'):
            try:
                c.execute(i)  
            except:
                pass 
    conn.commit()


sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\x_www_bi_ads_ds_jd_data.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\x_www_bi_ads_ds_jd_data_compare.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\x_erp_jd_dwd_dim_cost.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\x_erp_jd_ads_addorders.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\key_inventory.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\key_cangku.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\key_product.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\key_product_launchtime.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\key_product_sales.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\key_product_sales_fc.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\key_product_sales_fc_cpgl.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\key_product_sales_fc_cpgl_classify.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\key_product_sales_area.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\key_product_sales_area_cpgl.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\key_product_sales_area_cpgl_classify.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\key_product_sales_area_cpgl_area.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\key_product_sales_area_cpgl_customer.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\key_product_repurchase.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\key_product_repurchase_t.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\key_warehouse_diff.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\wlgzs_hxzy_gsyj_zx.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\wlgzs_hxzy_gsyj_zx_lastweek.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\wlgzs_hxzy_gsyj_zx_lastmonth.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\wlgzs_hxzy_gsyj_zx_last2month.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\wlgzs_hxzy_gsyj_zx_aftermonth.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\classify_year.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\dszc_key_product_year.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\dlzy_inventory.sql')
c.close()
conn.close()
