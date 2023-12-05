# -*- coding: utf-8 -*-
# 测试环境: python3.9.6

# day refresh 14:30:00
import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

from key_tab import sqlrun


sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\x_erp_jd_dws_balance.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\x_erp_jd_ads_balance_1122_06.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\x_erp_jd_dws_voucher_merge.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\x_erp_jd_ads_closebalance.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\x_erp_jd_dws_factory_dayend.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\wuliaomc_merge.sql')
sqlrun(r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\sql_clean\customer_valid.sql')

