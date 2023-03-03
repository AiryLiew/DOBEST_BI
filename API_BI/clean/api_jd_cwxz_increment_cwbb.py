# -*- coding: utf-8 -*-
# 测试环境: python3.9.6

import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

from api_cwzx_append import a_func,a1_func
from api_jd import func,func_wjg
from datetime import datetime

a1_func("http://10.225.137.124:7772/ZyyxDSS/GetTGLACCTAGEBALANCE",  'erp_jd_ods_dim_acctagebalance_cwzx' ,'fdetailid')
a1_func("http://10.225.137.124:7772/ZyyxDSS/GetTGLVOUCHERENTRY", 'erp_jd_ods_dim_voucherentry_cwzx' ,'fentryid')

a_func("http://10.225.137.124:7772/ZyyxDSS/GetTGLVOUCHER", "杭州游卡文化创意有限公司",'erp_jd_ods_dim_voucher_wc_cwzx'          ,'fVoucherID')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetTGLVOUCHER", "杭州泳淳网络技术有限公司",'erp_jd_ods_dim_voucher_yc_cwzx'          ,'fVoucherID') 
a_func("http://10.225.137.124:7772/ZyyxDSS/GetTGLVOUCHER", "杭州迷思文化创意有限公司",'erp_jd_ods_dim_voucher_ms_cwzx'          ,'fVoucherID')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetTGLVOUCHER", "上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_voucher_kyk_cwzx'          ,'fVoucherID')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetTGLVOUCHER", "上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_voucher_kyok_cwzx'          ,'fVoucherID')



# 每日全量
func("http://10.225.137.124:7772/ZyyxDSS/GetYingFuD",   "财务数据中心",'erp_jd_ods_dim_voucherpayable_wc_cwzx',   'erp_jd_ods_dim_voucherpayable_yc_cwzx',   'erp_jd_ods_dim_voucherpayable_ms_cwzx' ,'erp_jd_ods_dim_voucherpayable_kyk_cwzx' , 'erp_jd_ods_dim_voucherpayable_kyok_cwzx')
# func("http://10.225.137.124:7772/ZyyxDSS/GetTBDACCOUNT",   "财务数据中心",'erp_jd_ods_fact_account_wc_cwzx',   'erp_jd_ods_fact_account_yc_cwzx',   'erp_jd_ods_fact_account_ms_cwzx', 'erp_jd_ods_fact_account_kyk_cwzx' , 'erp_jd_ods_fact_account_kyok_cwzx' )

# func_wjg('http://10.225.137.124:7772/ZyyxDSS/GetTBDFLEXITEMDETAILV',"财务数据中心",'erp_jd_ods_fact_flexitemdetailv_cwzx')
# func_wjg('http://10.225.137.124:7772/ZyyxDSS/GetTBDACCOUNTL',"财务数据中心",'erp_jd_ods_fact_accountl_cwzx')
# func_wjg('http://10.225.137.124:7772/ZyyxDSS/GetTBDACCOUNTBOOKL',"财务数据中心",'erp_jd_ods_fact_accountbookl_cwzx')


print("\n","END CWBB", datetime.now(),"\n")