# -*- coding: utf-8 -*-
# 测试环境: python3.9.6

import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

from api_jd import func,func_QTCK,func_wjg, func_sjzx

# func("http://10.225.137.124:7772/ZyyxDSS/GetKeHu",       "财务数据中心",'erp_jd_ods_fact_client_wc_cwzx',          'erp_jd_ods_fact_client_yc_cwzx',          'erp_jd_ods_fact_client_ms_cwzx' ,'erp_jd_ods_fact_client_kyk_cwzx' ,'erp_jd_ods_fact_client_kyok_cwzx'         )
# func("http://10.225.137.124:7772/ZyyxDSS/GetWuLiao",     "财务数据中心",'erp_jd_ods_fact_classify_wc_cwzx',        'erp_jd_ods_fact_classify_yc_cwzx',        'erp_jd_ods_fact_classify_ms_cwzx'   ,'erp_jd_ods_fact_classify_kyk_cwzx'  ,'erp_jd_ods_fact_classify_kyok_cwzx'   )
# func("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouDD",   "财务数据中心",'erp_jd_ods_dim_purchaseorders_wc_cwzx',   'erp_jd_ods_dim_purchaseorders_yc_cwzx',   'erp_jd_ods_dim_purchaseorders_ms_cwzx' ,'erp_jd_ods_dim_purchaseorders_kyk_cwzx','erp_jd_ods_dim_purchaseorders_kyok_cwzx')
# func("http://10.225.137.124:7772/ZyyxDSS/GetYingFuD",   "财务数据中心",'erp_jd_ods_dim_voucherpayable_wc_cwzx',   'erp_jd_ods_dim_voucherpayable_yc_cwzx',   'erp_jd_ods_dim_voucherpayable_ms_cwzx' ,'erp_jd_ods_dim_voucherpayable_kyk_cwzx','erp_jd_ods_dim_voucherpayable_kyok_cwzx')
# func("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouDD",   "财务数据中心",'erp_jd_ods_dim_saleorders_wc_cwzx',   'erp_jd_ods_dim_saleorders_yc_cwzx',   'erp_jd_ods_dim_saleorders_ms_cwzx' ,'erp_jd_ods_dim_saleorders_kyk_cwzx','erp_jd_ods_dim_saleorders_kyok_cwzx')
# func("http://10.225.137.124:7772/ZyyxDSS/GetShouKuanD",   "财务数据中心",'erp_jd_ods_dim_proceeds_wc_cwzx',   'erp_jd_ods_dim_proceeds_yc_cwzx',   'erp_jd_ods_dim_proceeds_ms_cwzx' ,'erp_jd_ods_dim_proceeds_kyk_cwzx','erp_jd_ods_dim_proceeds_kyok_cwzx')
# func("http://10.225.137.124:7772/ZyyxDSS/GetTBDACCOUNT",   "财务数据中心",'erp_jd_ods_fact_account_wc_cwzx',   'erp_jd_ods_fact_account_yc_cwzx',   'erp_jd_ods_fact_account_ms_cwzx','erp_jd_ods_fact_account_kyk_cwzx' ,'erp_jd_ods_fact_account_kyok_cwzx')

# func_QTCK("财务数据中心",'erp_jd_ods_dim_othersshipping_wc_cwzx',   'erp_jd_ods_dim_othersshipping_yc_cwzx',   'erp_jd_ods_dim_othersshipping_ms_cwzx'  ,'erp_jd_ods_dim_othersshipping_kyk_cwzx' ,'erp_jd_ods_dim_othersshipping_kyok_cwzx')

# func_wjg('http://10.225.137.124:7772/ZyyxDSS/GetTBDFLEXITEMDETAILV',"财务数据中心",'erp_jd_ods_fact_flexitemdetailv_cwzx')
# func_wjg('http://10.225.137.124:7772/ZyyxDSS/GetTBDACCOUNTL',"财务数据中心",'erp_jd_ods_fact_accountl_cwzx')
# func_wjg('http://10.225.137.124:7772/ZyyxDSS/GetFuKuanSQD',"财务数据中心",'erp_jd_ods_dim_prepayment_cwzx')
# func_wjg("http://10.225.137.124:7772/ZyyxDSS/GetTGLBALANCE","财务数据中心",'erp_jd_ods_dim_balance_cwzx')
# func_wjg('http://10.225.137.124:7772/ZyyxDSS/GetTBDACCOUNTBOOKL',"财务数据中心",'erp_jd_ods_fact_accountbookl_cwzx')
# func_wjg("http://10.225.137.124:7772/ZyyxDSS/GetTGLACCTAGEBALANCE",   "财务数据中心",'erp_jd_ods_dim_acctagebalance_cwzx' )
# func_wjg("http://10.225.137.124:7772/ZyyxDSS/GetTGLVOUCHERENTRY",   "财务数据中心",'erp_jd_ods_dim_voucherentry_cwzx' )
# func_wjg("http://10.225.137.124:7772/ZyyxDSS/GetTBDVOUCHERGROUPL",   "财务数据中心",'erp_jd_ods_fact_vouchergroupl_cwzx' )

# func_sjzx('http://10.225.137.124:7772/ZyyxDSS/GetTBASASSISTANTDATAENTRY',"财务数据中心",'erp_jd_ods_fact_assistantdataentry_cwzx')
# func_sjzx('http://10.225.137.124:7772/ZyyxDSS/GetTMETALOOKUPCLASS',"财务数据中心",'erp_jd_ods_fact_lookupclass_cwzx')
# func_sjzx('http://10.225.137.124:7772/ZyyxDSS/GetTBDFLEXITEMPROPERTY',"财务数据中心",'erp_jd_ods_fact_flexitemproerty_cwzx')