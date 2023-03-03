# -*- coding: utf-8 -*-
# 测试环境: python3.9.6

import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

from api_jd import *

# func("http://10.225.137.124:7772/ZyyxDSS/GetKeHu",       "DOBEST",'erp_jd_ods_fact_client_wc_dobest',          'erp_jd_ods_fact_client_yc_dobest',          'erp_jd_ods_fact_client_ms_dobest'    ,    'erp_jd_ods_fact_client_kyk_dobest' ,    'erp_jd_ods_fact_client_kyok_dobest'  )
# func("http://10.225.137.124:7772/ZyyxDSS/GetWuLiao",     "DOBEST",'erp_jd_ods_fact_classify_wc_dobest',        'erp_jd_ods_fact_classify_yc_dobest',        'erp_jd_ods_fact_classify_ms_dobest'    ,        'erp_jd_ods_fact_classify_kyk_dobest'   ,        'erp_jd_ods_fact_classify_kyok_dobest'   )
# func("http://10.225.137.124:7772/ZyyxDSS/GetPanKuiD",    "DOBEST",'erp_jd_ods_dim_inventoryloss_wc_dobest',    'erp_jd_ods_dim_inventoryloss_yc_dobest',    'erp_jd_ods_dim_inventoryloss_ms_dobest' ,    'erp_jd_ods_dim_inventoryloss_kyk_dobest',    'erp_jd_ods_dim_inventoryloss_kyok_dobest'   )
# func("http://10.225.137.124:7772/ZyyxDSS/GetQiTaRKD",    "DOBEST",'erp_jd_ods_dim_othersreceiving_wc_dobest',  'erp_jd_ods_dim_othersreceiving_yc_dobest',  'erp_jd_ods_dim_othersreceiving_ms_dobest' ,  'erp_jd_ods_dim_othersreceiving_kyk_dobest',  'erp_jd_ods_dim_othersreceiving_kyok_dobest' )
# func("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouDD",   "DOBEST",'erp_jd_ods_dim_purchaseorders_wc_dobest',   'erp_jd_ods_dim_purchaseorders_yc_dobest',   'erp_jd_ods_dim_purchaseorders_ms_dobest'   ,   'erp_jd_ods_dim_purchaseorders_kyk_dobest',   'erp_jd_ods_dim_purchaseorders_kyok_dobest'  )
# func("http://10.225.137.124:7772/ZyyxDSS/GetPanYingD",   "DOBEST",'erp_jd_ods_dim_inventoryprofit_wc_dobest',  'erp_jd_ods_dim_inventoryprofit_yc_dobest',  'erp_jd_ods_dim_inventoryprofit_ms_dobest'  ,  'erp_jd_ods_dim_inventoryprofit_kyk_dobest' ,  'erp_jd_ods_dim_inventoryprofit_kyok_dobest' )
# func("http://10.225.137.124:7772/ZyyxDSS/GetJiShouJSD",  "DOBEST",'erp_jd_ods_dim_consignment_wc_dobest',      'erp_jd_ods_dim_consignment_yc_dobest',      'erp_jd_ods_dim_consignment_ms_dobest'    ,      'erp_jd_ods_dim_consignment_kyk_dobest',      'erp_jd_ods_dim_consignment_kyok_dobest'  )
# func("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouTLD",  "DOBEST",'erp_jd_ods_dim_purchasereturn_wc_dobest',   'erp_jd_ods_dim_purchasereturn_yc_dobest',   'erp_jd_ods_dim_purchasereturn_ms_dobest' ,   'erp_jd_ods_dim_purchasereturn_kyk_dobest',   'erp_jd_ods_dim_purchasereturn_kyok_dobest'  )
# func("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouRKD",  "DOBEST",'erp_jd_ods_dim_purchasereceiving_wc_dobest','erp_jd_ods_dim_purchasereceiving_yc_dobest','erp_jd_ods_dim_purchasereceiving_ms_dobest','erp_jd_ods_dim_purchasereceiving_kyk_dobest','erp_jd_ods_dim_purchasereceiving_kyok_dobest')
# func("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouDD", "DOBEST",'erp_jd_ods_dim_saleorders_wc_dobest',       'erp_jd_ods_dim_saleorders_yc_dobest',       'erp_jd_ods_dim_saleorders_ms_dobest'   ,       'erp_jd_ods_dim_saleorders_kyk_dobest'  ,       'erp_jd_ods_dim_saleorders_kyok_dobest'      )
# func("http://10.225.137.124:7772/ZyyxDSS/GetWuLiaoSFHZ", "DOBEST",'erp_jd_ods_dim_monthendinventory_wc_dobest','erp_jd_ods_dim_monthendinventory_yc_dobest','erp_jd_ods_dim_monthendinventory_ms_dobest',    'erp_jd_ods_dim_monthendinventory_kyk_dobest',    'erp_jd_ods_dim_monthendinventory_kyok_dobest')
# func("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouTHD","DOBEST",'erp_jd_ods_dim_salereturn_wc_dobest',       'erp_jd_ods_dim_salereturn_yc_dobest',       'erp_jd_ods_dim_salereturn_ms_dobest' ,       'erp_jd_ods_dim_salereturn_kyk_dobest'   ,       'erp_jd_ods_dim_salereturn_kyok_dobest'     )
# func("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouCKD","DOBEST",'erp_jd_ods_dim_saleshipping_wc_dobest',     'erp_jd_ods_dim_saleshipping_yc_dobest',     'erp_jd_ods_dim_saleshipping_ms_dobest' ,     'erp_jd_ods_dim_saleshipping_kyk_dobest'   ,     'erp_jd_ods_dim_saleshipping_kyok_dobest'   )
# func("http://10.225.137.124:7772/ZyyxDSS/GetZuZhuangCX", "DOBEST",'erp_jd_ods_dim_assemble_wc_dobest',         'erp_jd_ods_dim_assemble_yc_dobest',         'erp_jd_ods_dim_assemble_ms_dobest'   ,         'erp_jd_ods_dim_assemble_kyk_dobest' ,         'erp_jd_ods_dim_assemble_kyok_dobest'       )
# func("http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDRD",  "DOBEST",'erp_jd_ods_dim_distributedin_wc_dobest',    'erp_jd_ods_dim_distributedin_yc_dobest',    'erp_jd_ods_dim_distributedin_ms_dobest'  ,    'erp_jd_ods_dim_distributedin_kyk_dobest' ,    'erp_jd_ods_dim_distributedin_kyok_dobest'   )
# func("http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDCD",  "DOBEST",'erp_jd_ods_dim_distributedout_wc_dobest',   'erp_jd_ods_dim_distributedout_yc_dobest',   'erp_jd_ods_dim_distributedout_ms_dobest' ,   'erp_jd_ods_dim_distributedout_kyk_dobest' ,   'erp_jd_ods_dim_distributedout_kyok_dobest'   )
# func("http://10.225.137.124:7772/ZyyxDSS/GetZhiJieDBD",  "DOBEST",'erp_jd_ods_dim_allocation_wc_dobest',       'erp_jd_ods_dim_allocation_yc_dobest',       'erp_jd_ods_dim_allocation_ms_dobest'    ,       'erp_jd_ods_dim_allocation_kyk_dobest'   ,       'erp_jd_ods_dim_allocation_kyok_dobest'  )
# func("http://10.225.137.124:7772/ZyyxDSS/GetYingFuD",    "DOBEST",'erp_jd_ods_dim_voucherpayable_wc_dobest',   'erp_jd_ods_dim_voucherpayable_yc_dobest',   'erp_jd_ods_dim_voucherpayable_ms_dobest' ,   'erp_jd_ods_dim_voucherpayable_kyk_dobest',   'erp_jd_ods_dim_voucherpayable_kyok_dobest'  )
# func("http://10.225.137.124:7772/ZyyxDSS/GetShouKuanD",  "DOBEST",'erp_jd_ods_dim_proceeds_wc_dobest',         'erp_jd_ods_dim_proceeds_yc_dobest',         'erp_jd_ods_dim_proceeds_ms_dobest',         'erp_jd_ods_dim_proceeds_kyk_dobest',         'erp_jd_ods_dim_proceeds_kyok_dobest' )
# func("http://10.225.137.124:7772/ZyyxDSS/GetTGLVOUCHER",   "DOBEST",'erp_jd_ods_dim_voucher_wc_dobest',        'erp_jd_ods_dim_voucher_yc_dobest',          'erp_jd_ods_dim_voucher_ms_dobest' ,          'erp_jd_ods_dim_voucher_kyk_dobest' ,          'erp_jd_ods_dim_voucher_kyok_dobest' )
# func("http://10.225.137.124:7772/ZyyxDSS/GetTBDACCOUNT",   "DOBEST",'erp_jd_ods_fact_account_wc_dobest',        'erp_jd_ods_fact_account_yc_dobest',          'erp_jd_ods_fact_account_ms_dobest' ,          'erp_jd_ods_fact_account_kyk_dobest',          'erp_jd_ods_fact_account_kyok_dobest')

# func_QTCK("DOBEST",'erp_jd_ods_dim_othersshipping_wc_dobest',   'erp_jd_ods_dim_othersshipping_yc_dobest',   'erp_jd_ods_dim_othersshipping_ms_dobest'  ,       'erp_jd_ods_dim_othersshipping_kyk_dobest'  ,       'erp_jd_ods_dim_othersshipping_kyok_dobest' )

func_wjg('http://10.225.137.124:7772/ZyyxDSS/GetFuKuanSQD',"DOBEST",'erp_jd_ods_dim_prepayment_dobest')
# func_wjg("http://10.225.137.124:7772/ZyyxDSS/GetTGLACCTAGEBALANCE",   "DOBEST",'erp_jd_ods_dim_acctagebalance_dobest' )
# func_wjg("http://10.225.137.124:7772/ZyyxDSS/GetTGLBALANCE",   "DOBEST",'erp_jd_ods_dim_balance_dobest' )
# func_wjg("http://10.225.137.124:7772/ZyyxDSS/GetTGLVOUCHERENTRY",   "DOBEST",'erp_jd_ods_dim_voucherentry_dobest' )
# func_wjg("http://10.225.137.124:7772/ZyyxDSS/GetTBDFLEXITEMDETAILV",   "DOBEST",'erp_jd_ods_fact_flexitemdetailv_dobest' )
# func_wjg("http://10.225.137.124:7772/ZyyxDSS/GetTBDVOUCHERGROUPL",   "DOBEST",'erp_jd_ods_fact_vouchergroupl_dobest' )
# func_wjg('http://10.225.137.124:7772/ZyyxDSS/GetTBDACCOUNTL',"DOBEST",'erp_jd_ods_fact_accountl_dobest')
# func_wjg('http://10.225.137.124:7772/ZyyxDSS/GetTBDACCOUNTBOOKL',"DOBEST",'erp_jd_ods_fact_accountbookl_dobest')

# func_sjzx('http://10.225.137.124:7772/ZyyxDSS/GetTBASASSISTANTDATAENTRY',"DOBEST",'erp_jd_ods_fact_assistantdataentry_dobest')
# func_sjzx('http://10.225.137.124:7772/ZyyxDSS/GetTBDFLEXITEMPROPERTY',"DOBEST",'erp_jd_ods_fact_flexitemproperty_dobest')
# func_sjzx('http://10.225.137.124:7772/ZyyxDSS/GetTMETALOOKUPCLASS',"DOBEST",'erp_jd_ods_fact_lookupclass_dobest')



# func("http://10.225.137.124:7772/ZyyxDSS/GetKeHu",       "项目公司数据中心",'erp_jd_ods_fact_client_wc_xmgs',          'erp_jd_ods_fact_client_yc_xmgs',          'erp_jd_ods_fact_client_ms_xmgs'        ,'erp_jd_ods_fact_client_kyk_xmgs'   ,'erp_jd_ods_fact_client_kyok_xmgs'  )
# func("http://10.225.137.124:7772/ZyyxDSS/GetWuLiao",     "项目公司数据中心",'erp_jd_ods_fact_classify_wc_xmgs',        'erp_jd_ods_fact_classify_yc_xmgs',        'erp_jd_ods_fact_classify_ms_xmgs'  ,        'erp_jd_ods_fact_classify_kyk_xmgs'   ,        'erp_jd_ods_fact_classify_kyok_xmgs'       )
# func("http://10.225.137.124:7772/ZyyxDSS/GetPanKuiD",    "项目公司数据中心",'erp_jd_ods_dim_inventoryloss_wc_xmgs',    'erp_jd_ods_dim_inventoryloss_yc_xmgs',    'erp_jd_ods_dim_inventoryloss_ms_xmgs'  ,    'erp_jd_ods_dim_inventoryloss_kyk_xmgs' ,    'erp_jd_ods_dim_inventoryloss_kyok_xmgs'   )
# func("http://10.225.137.124:7772/ZyyxDSS/GetQiTaRKD",    "项目公司数据中心",'erp_jd_ods_dim_othersreceiving_wc_xmgs',  'erp_jd_ods_dim_othersreceiving_yc_xmgs',  'erp_jd_ods_dim_othersreceiving_ms_xmgs' ,  'erp_jd_ods_dim_othersreceiving_kyk_xmgs',  'erp_jd_ods_dim_othersreceiving_kyok_xmgs' )
# func("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouDD",   "项目公司数据中心",'erp_jd_ods_dim_purchaseorders_wc_xmgs',   'erp_jd_ods_dim_purchaseorders_yc_xmgs',   'erp_jd_ods_dim_purchaseorders_ms_xmgs'   ,   'erp_jd_ods_dim_purchaseorders_kyk_xmgs' ,   'erp_jd_ods_dim_purchaseorders_kyok_xmgs')
# func("http://10.225.137.124:7772/ZyyxDSS/GetPanYingD",   "项目公司数据中心",'erp_jd_ods_dim_inventoryprofit_wc_xmgs',  'erp_jd_ods_dim_inventoryprofit_yc_xmgs',  'erp_jd_ods_dim_inventoryprofit_ms_xmgs' ,  'erp_jd_ods_dim_inventoryprofit_kyk_xmgs' ,  'erp_jd_ods_dim_inventoryprofit_kyok_xmgs'  )
# func("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouRKD",  "项目公司数据中心",'erp_jd_ods_dim_purchasereceiving_wc_xmgs','erp_jd_ods_dim_purchasereceiving_yc_xmgs','erp_jd_ods_dim_purchasereceiving_ms_xmgs','erp_jd_ods_dim_purchasereceiving_kyk_xmgs','erp_jd_ods_dim_purchasereceiving_kyok_xmgs')
# func("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouTLD",  "项目公司数据中心",'erp_jd_ods_dim_purchasereturn_wc_xmgs',   'erp_jd_ods_dim_purchasereturn_yc_xmgs',   'erp_jd_ods_dim_purchasereturn_ms_xmgs' ,   'erp_jd_ods_dim_purchasereturn_kyk_xmgs'  ,   'erp_jd_ods_dim_purchasereturn_kyok_xmgs'    )
# func("http://10.225.137.124:7772/ZyyxDSS/GetJiShouJSD",  "项目公司数据中心",'erp_jd_ods_dim_consignment_wc_xmgs',      'erp_jd_ods_dim_consignment_yc_xmgs',      'erp_jd_ods_dim_consignment_ms_xmgs' ,      'erp_jd_ods_dim_consignment_kyk_xmgs' ,      'erp_jd_ods_dim_consignment_kyok_xmgs'      )
# func("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouDD", "项目公司数据中心",'erp_jd_ods_dim_saleorders_wc_xmgs',       'erp_jd_ods_dim_saleorders_yc_xmgs',       'erp_jd_ods_dim_saleorders_ms_xmgs'  ,       'erp_jd_ods_dim_saleorders_kyk_xmgs' ,       'erp_jd_ods_dim_saleorders_kyok_xmgs'      )
# func("http://10.225.137.124:7772/ZyyxDSS/GetWuLiaoSFHZ", "项目公司数据中心",'erp_jd_ods_dim_monthendinventory_wc_xmgs','erp_jd_ods_dim_monthendinventory_yc_xmgs','erp_jd_ods_dim_monthendinventory_ms_xmgs',    'erp_jd_ods_dim_monthendinventory_kyk_xmgs',    'erp_jd_ods_dim_monthendinventory_kyok_xmgs')
# func("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouTHD","项目公司数据中心",'erp_jd_ods_dim_salereturn_wc_xmgs',       'erp_jd_ods_dim_salereturn_yc_xmgs',       'erp_jd_ods_dim_salereturn_ms_xmgs',       'erp_jd_ods_dim_salereturn_kyk_xmgs'  ,       'erp_jd_ods_dim_salereturn_kyok_xmgs'    )
# func("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouCKD","项目公司数据中心",'erp_jd_ods_dim_saleshipping_wc_xmgs',     'erp_jd_ods_dim_saleshipping_yc_xmgs',     'erp_jd_ods_dim_saleshipping_ms_xmgs',     'erp_jd_ods_dim_saleshipping_kyk_xmgs'   ,     'erp_jd_ods_dim_saleshipping_kyok_xmgs'   )
# func("http://10.225.137.124:7772/ZyyxDSS/GetZuZhuangCX", "项目公司数据中心",'erp_jd_ods_dim_assemble_wc_xmgs',         'erp_jd_ods_dim_assemble_yc_xmgs',         'erp_jd_ods_dim_assemble_ms_xmgs'  ,         'erp_jd_ods_dim_assemble_kyk_xmgs',         'erp_jd_ods_dim_assemble_kyok_xmgs'       )
# func("http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDRD",  "项目公司数据中心",'erp_jd_ods_dim_distributedin_wc_xmgs',    'erp_jd_ods_dim_distributedin_yc_xmgs',    'erp_jd_ods_dim_distributedin_ms_xmgs' ,    'erp_jd_ods_dim_distributedin_kyk_xmgs',    'erp_jd_ods_dim_distributedin_kyok_xmgs'   )
# func("http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDCD",  "项目公司数据中心",'erp_jd_ods_dim_distributedout_wc_xmgs',   'erp_jd_ods_dim_distributedout_yc_xmgs',   'erp_jd_ods_dim_distributedout_ms_xmgs'  ,   'erp_jd_ods_dim_distributedout_kyk_xmgs' ,   'erp_jd_ods_dim_distributedout_kyok_xmgs'  )
# func("http://10.225.137.124:7772/ZyyxDSS/GetZhiJieDBD",  "项目公司数据中心",'erp_jd_ods_dim_allocation_wc_xmgs',       'erp_jd_ods_dim_allocation_yc_xmgs',       'erp_jd_ods_dim_allocation_ms_xmgs'       ,       'erp_jd_ods_dim_allocation_kyk_xmgs'  ,       'erp_jd_ods_dim_allocation_kyok_xmgs' )
# func("http://10.225.137.124:7772/ZyyxDSS/GetYingFuD",    "项目公司数据中心",'erp_jd_ods_dim_voucherpayable_wc_xmgs',   'erp_jd_ods_dim_voucherpayable_yc_xmgs',   'erp_jd_ods_dim_voucherpayable_ms_xmgs' ,   'erp_jd_ods_dim_voucherpayable_kyk_xmgs',   'erp_jd_ods_dim_voucherpayable_kyok_xmgs'  )
# func("http://10.225.137.124:7772/ZyyxDSS/GetShouKuanD",  "项目公司数据中心",'erp_jd_ods_dim_proceeds_wc_xmgs',         'erp_jd_ods_dim_proceeds_yc_xmgs',         'erp_jd_ods_dim_proceeds_ms_xmgs',         'erp_jd_ods_dim_proceeds_kyk_xmgs',         'erp_jd_ods_dim_proceeds_kyok_xmgs' )
# func("http://10.225.137.124:7772/ZyyxDSS/GetTGLVOUCHER",   "项目公司数据中心",'erp_jd_ods_dim_voucher_wc_xmgs',        'erp_jd_ods_dim_voucher_yc_xmgs',          'erp_jd_ods_dim_voucher_ms_xmgs',          'erp_jd_ods_dim_voucher_kyk_xmgs',          'erp_jd_ods_dim_voucher_kyok_xmgs' )
# func("http://10.225.137.124:7772/ZyyxDSS/GetTBDACCOUNT",   "项目公司数据中心",'erp_jd_ods_fact_account_wc_xmgs',        'erp_jd_ods_fact_account_yc_xmgs',          'erp_jd_ods_fact_account_ms_xmgs' ,          'erp_jd_ods_fact_account_kyk_xmgs',          'erp_jd_ods_fact_account_kyok_xmgs')

# func_QTCK("项目公司数据中心",'erp_jd_ods_dim_othersshipping_wc_xmgs',   'erp_jd_ods_dim_othersshipping_yc_xmgs',   'erp_jd_ods_dim_othersshipping_ms_xmgs'   ,       'erp_jd_ods_dim_othersshipping_kyk_xmgs',       'erp_jd_ods_dim_othersshipping_kyok_xmgs' )

# func_wjg('http://10.225.137.124:7772/ZyyxDSS/GetFuKuanSQD',"项目公司数据中心",'erp_jd_ods_dim_prepayment_xmgs')
# func_wjg("http://10.225.137.124:7772/ZyyxDSS/GetTGLACCTAGEBALANCE",   "项目公司数据中心",'erp_jd_ods_dim_acctagebalance_xmgs' )
# func_wjg("http://10.225.137.124:7772/ZyyxDSS/GetTGLBALANCE",   "项目公司数据中心",'erp_jd_ods_dim_balance_xmgs' )
# func_wjg("http://10.225.137.124:7772/ZyyxDSS/GetTGLVOUCHERENTRY",   "项目公司数据中心",'erp_jd_ods_dim_voucherentry_xmgs' )
# func_wjg("http://10.225.137.124:7772/ZyyxDSS/GetTBDFLEXITEMDETAILV",   "项目公司数据中心",'erp_jd_ods_fact_flexitemdetailv_xmgs' )
# func_wjg("http://10.225.137.124:7772/ZyyxDSS/GetTBDVOUCHERGROUPL",   "项目公司数据中心",'erp_jd_ods_fact_vouchergroupl_xmgs' )
# func_wjg('http://10.225.137.124:7772/ZyyxDSS/GetTBDACCOUNTL',"项目公司数据中心",'erp_jd_ods_fact_accountl_xmgs')
# func_wjg('http://10.225.137.124:7772/ZyyxDSS/GetTBDACCOUNTBOOKL',"项目公司数据中心",'erp_jd_ods_fact_accountbookl_xmgs')

# func_sjzx('http://10.225.137.124:7772/ZyyxDSS/GetTMETALOOKUPCLASS',"项目公司数据中心",'erp_jd_ods_fact_lookupclass_xmgs')
# func_sjzx('http://10.225.137.124:7772/ZyyxDSS/GetTBASASSISTANTDATAENTRY',"项目公司数据中心",'erp_jd_ods_fact_assistantdataentry_xmgs')
# func_sjzx('http://10.225.137.124:7772/ZyyxDSS/GetTBDFLEXITEMPROPERTY',"项目公司数据中心",'erp_jd_ods_fact_flexitemproperty_xmgs')
