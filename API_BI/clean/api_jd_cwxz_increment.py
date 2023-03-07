# -*- coding: utf-8 -*-
# 测试环境: python3.9.6

import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

from api_cwzx_append import a_func,a1_func,s1_funcB
from api_jd import func,func_QTCK,func_wjg, func_sjzx


# 每日增量
a1_func("http://10.225.137.124:7772/ZyyxDSS/GetTGLACCTAGEBALANCE", 'erp_jd_ods_dim_acctagebalance_cwzx','fdetailid')
a1_func("http://10.225.137.124:7772/ZyyxDSS/GetFuKuanSQD",         'erp_jd_ods_dim_prepayment_cwzx',    'fid')
a1_func("http://10.225.137.124:7772/ZyyxDSS/GetTGLBALANCE",        'erp_jd_ods_dim_balance_cwzx',       'fAccountID')
a1_func("http://10.225.137.124:7772/ZyyxDSS/GetTGLVOUCHERENTRY",   'erp_jd_ods_dim_voucherentry_cwzx',  'fentryid')


a_func("http://10.225.137.124:7772/ZyyxDSS/GetQiTaRKD",    "杭州游卡文化创意有限公司",'erp_jd_ods_dim_othersreceiving_wc_cwzx'  ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetPanKuiD",    "杭州游卡文化创意有限公司",'erp_jd_ods_dim_inventoryloss_wc_cwzx'    ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetPanYingD",   "杭州游卡文化创意有限公司",'erp_jd_ods_dim_inventoryprofit_wc_cwzx'  ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouRKD",  "杭州游卡文化创意有限公司",'erp_jd_ods_dim_purchasereceiving_wc_cwzx','fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetJiShouJSD",  "杭州游卡文化创意有限公司",'erp_jd_ods_dim_consignment_wc_cwzx'      ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouTLD",  "杭州游卡文化创意有限公司",'erp_jd_ods_dim_purchasereturn_wc_cwzx'   ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouTHD","杭州游卡文化创意有限公司",'erp_jd_ods_dim_salereturn_wc_cwzx'       ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouCKD","杭州游卡文化创意有限公司",'erp_jd_ods_dim_saleshipping_wc_cwzx'     ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetZuZhuangCX", "杭州游卡文化创意有限公司",'erp_jd_ods_dim_assemble_wc_cwzx'         ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDRD",  "杭州游卡文化创意有限公司",'erp_jd_ods_dim_distributedin_wc_cwzx'    ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDCD",  "杭州游卡文化创意有限公司",'erp_jd_ods_dim_distributedout_wc_cwzx'   ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetZhiJieDBD",  "杭州游卡文化创意有限公司",'erp_jd_ods_dim_allocation_wc_cwzx'       ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetTGLVOUCHER", "杭州游卡文化创意有限公司",'erp_jd_ods_dim_voucher_wc_cwzx'          ,'fVoucherID')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetWuLiaoSFHZ", "杭州游卡文化创意有限公司",'erp_jd_ods_dim_monthendinventory_wc_cwzx','fid')

a_func("http://10.225.137.124:7772/ZyyxDSS/GetWuLiaoSFHZ", "杭州泳淳网络技术有限公司",'erp_jd_ods_dim_monthendinventory_yc_cwzx','fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetQiTaRKD",    "杭州泳淳网络技术有限公司",'erp_jd_ods_dim_othersreceiving_yc_cwzx'  ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetPanKuiD",    "杭州泳淳网络技术有限公司",'erp_jd_ods_dim_inventoryloss_yc_cwzx'    ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetPanYingD",   "杭州泳淳网络技术有限公司",'erp_jd_ods_dim_inventoryprofit_yc_cwzx'  ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouRKD",  "杭州泳淳网络技术有限公司",'erp_jd_ods_dim_purchasereceiving_yc_cwzx','fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetJiShouJSD",  "杭州泳淳网络技术有限公司",'erp_jd_ods_dim_consignment_yc_cwzx'      ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouTLD",  "杭州泳淳网络技术有限公司",'erp_jd_ods_dim_purchasereturn_yc_cwzx'   ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouTHD","杭州泳淳网络技术有限公司",'erp_jd_ods_dim_salereturn_yc_cwzx'       ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouCKD","杭州泳淳网络技术有限公司",'erp_jd_ods_dim_saleshipping_yc_cwzx'     ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetZuZhuangCX", "杭州泳淳网络技术有限公司",'erp_jd_ods_dim_assemble_yc_cwzx'         ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDRD",  "杭州泳淳网络技术有限公司",'erp_jd_ods_dim_distributedin_yc_cwzx'    ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDCD",  "杭州泳淳网络技术有限公司",'erp_jd_ods_dim_distributedout_yc_cwzx'   ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetZhiJieDBD",  "杭州泳淳网络技术有限公司",'erp_jd_ods_dim_allocation_yc_cwzx'       ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetTGLVOUCHER", "杭州泳淳网络技术有限公司",'erp_jd_ods_dim_voucher_yc_cwzx'          ,'fVoucherID') 

a_func("http://10.225.137.124:7772/ZyyxDSS/GetQiTaRKD",    "杭州迷思文化创意有限公司",'erp_jd_ods_dim_othersreceiving_ms_cwzx'  ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetPanKuiD",    "杭州迷思文化创意有限公司",'erp_jd_ods_dim_inventoryloss_ms_cwzx'    ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetPanYingD",   "杭州迷思文化创意有限公司",'erp_jd_ods_dim_inventoryprofit_ms_cwzx'  ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouRKD",  "杭州迷思文化创意有限公司",'erp_jd_ods_dim_purchasereceiving_ms_cwzx','fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetJiShouJSD",  "杭州迷思文化创意有限公司",'erp_jd_ods_dim_consignment_ms_cwzx'      ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouTLD",  "杭州迷思文化创意有限公司",'erp_jd_ods_dim_purchasereturn_ms_cwzx'   ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouTHD","杭州迷思文化创意有限公司",'erp_jd_ods_dim_salereturn_ms_cwzx'       ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouCKD","杭州迷思文化创意有限公司",'erp_jd_ods_dim_saleshipping_ms_cwzx'     ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetZuZhuangCX", "杭州迷思文化创意有限公司",'erp_jd_ods_dim_assemble_ms_cwzx'         ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDRD",  "杭州迷思文化创意有限公司",'erp_jd_ods_dim_distributedin_ms_cwzx'    ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDCD",  "杭州迷思文化创意有限公司",'erp_jd_ods_dim_distributedout_ms_cwzx'   ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetZhiJieDBD",  "杭州迷思文化创意有限公司",'erp_jd_ods_dim_allocation_ms_cwzx'       ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetTGLVOUCHER", "杭州迷思文化创意有限公司",'erp_jd_ods_dim_voucher_ms_cwzx'          ,'fVoucherID')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetWuLiaoSFHZ", "杭州迷思文化创意有限公司",'erp_jd_ods_dim_monthendinventory_ms_cwzx','fid')

a_func("http://10.225.137.124:7772/ZyyxDSS/GetQiTaRKD",    "上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_othersreceiving_kyk_cwzx','fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouRKD",  "上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_purchasereceiving_kyk_cwzx','fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetTGLVOUCHER", "上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_voucher_kyk_cwzx'          ,'fVoucherID')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouTLD",  "上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_purchasereturn_kyk_cwzx'   ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouTHD","上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_salereturn_kyk_cwzx'       ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouCKD","上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_saleshipping_kyk_cwzx'     ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetZuZhuangCX", "上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_assemble_kyk_cwzx'         ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetWuLiaoSFHZ", "上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_monthendinventory_kyk_cwzx','fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDRD",  "上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_distributedin_kyk_cwzx'    ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDCD",  "上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_distributedout_kyk_cwzx'   ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetZhiJieDBD",  "上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_allocation_kyk_cwzx'       ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetPanKuiD",    "上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_inventoryloss_kyk_cwzx'    ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetPanYingD",   "上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_inventoryprofit_kyk_cwzx'  ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetJiShouJSD",  "上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_consignment_kyk_cwzx'      ,'fid')



a_func("http://10.225.137.124:7772/ZyyxDSS/GetQiTaRKD",    "上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_othersreceiving_kyok_cwzx','fid'  )
a_func("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouRKD",  "上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_purchasereceiving_kyok_cwzx','fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetTGLVOUCHER", "上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_voucher_kyok_cwzx'          ,'fVoucherID')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouTLD",  "上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_purchasereturn_kyok_cwzx'   ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouTHD","上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_salereturn_kyok_cwzx'       ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouCKD","上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_saleshipping_kyok_cwzx'     ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetZuZhuangCX", "上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_assemble_kyok_cwzx'         ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetWuLiaoSFHZ", "上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_monthendinventory_kyok_cwzx','fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDRD",  "上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_distributedin_kyok_cwzx'    ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDCD",  "上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_distributedout_kyok_cwzx'   ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetZhiJieDBD",  "上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_allocation_kyok_cwzx'       ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetPanKuiD",    "上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_inventoryloss_kyok_cwzx'    ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetPanYingD",   "上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_inventoryprofit_kyok_cwzx'  ,'fid')
a_func("http://10.225.137.124:7772/ZyyxDSS/GetJiShouJSD",  "上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_consignment_kyok_cwzx'      ,'fid')






# 每日全量
func("http://10.225.137.124:7772/ZyyxDSS/GetKeHu",      "财务数据中心",'erp_jd_ods_fact_client_wc_cwzx',       'erp_jd_ods_fact_client_yc_cwzx',       'erp_jd_ods_fact_client_ms_cwzx',       'erp_jd_ods_fact_client_kyk_cwzx' ,       'erp_jd_ods_fact_client_kyok_cwzx'    )
func("http://10.225.137.124:7772/ZyyxDSS/GetWuLiao",    "财务数据中心",'erp_jd_ods_fact_classify_wc_cwzx',     'erp_jd_ods_fact_classify_yc_cwzx',     'erp_jd_ods_fact_classify_ms_cwzx',     'erp_jd_ods_fact_classify_kyk_cwzx',      'erp_jd_ods_fact_classify_kyok_cwzx'   )
func("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouDD",  "财务数据中心",'erp_jd_ods_dim_purchaseorders_wc_cwzx','erp_jd_ods_dim_purchaseorders_yc_cwzx','erp_jd_ods_dim_purchaseorders_ms_cwzx','erp_jd_ods_dim_purchaseorders_kyk_cwzx', 'erp_jd_ods_dim_purchaseorders_kyok_cwzx')
func("http://10.225.137.124:7772/ZyyxDSS/GetYingFuD",   "财务数据中心",'erp_jd_ods_dim_voucherpayable_wc_cwzx','erp_jd_ods_dim_voucherpayable_yc_cwzx','erp_jd_ods_dim_voucherpayable_ms_cwzx','erp_jd_ods_dim_voucherpayable_kyk_cwzx' ,'erp_jd_ods_dim_voucherpayable_kyok_cwzx')
func("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouDD","财务数据中心",'erp_jd_ods_dim_saleorders_wc_cwzx',    'erp_jd_ods_dim_saleorders_yc_cwzx',    'erp_jd_ods_dim_saleorders_ms_cwzx',    'erp_jd_ods_dim_saleorders_kyk_cwzx' ,    'erp_jd_ods_dim_saleorders_kyok_cwzx')
func("http://10.225.137.124:7772/ZyyxDSS/GetShouKuanD", "财务数据中心",'erp_jd_ods_dim_proceeds_wc_cwzx',      'erp_jd_ods_dim_proceeds_yc_cwzx',      'erp_jd_ods_dim_proceeds_ms_cwzx',      'erp_jd_ods_dim_proceeds_kyk_cwzx',       'erp_jd_ods_dim_proceeds_kyok_cwzx')
func("http://10.225.137.124:7772/ZyyxDSS/GetTBDACCOUNT","财务数据中心",'erp_jd_ods_fact_account_wc_cwzx',      'erp_jd_ods_fact_account_yc_cwzx',      'erp_jd_ods_fact_account_ms_cwzx',      'erp_jd_ods_fact_account_kyk_cwzx',       'erp_jd_ods_fact_account_kyok_cwzx' )

func_QTCK("财务数据中心",'erp_jd_ods_dim_othersshipping_wc_cwzx',   'erp_jd_ods_dim_othersshipping_yc_cwzx',   'erp_jd_ods_dim_othersshipping_ms_cwzx'  ,'erp_jd_ods_dim_othersshipping_kyk_cwzx','erp_jd_ods_dim_othersshipping_kyok_cwzx' )

func_wjg('http://10.225.137.124:7772/ZyyxDSS/GetTBDFLEXITEMDETAILV',"财务数据中心",'erp_jd_ods_fact_flexitemdetailv_cwzx')
func_wjg('http://10.225.137.124:7772/ZyyxDSS/GetTBDACCOUNTL',"财务数据中心",'erp_jd_ods_fact_accountl_cwzx')
func_wjg('http://10.225.137.124:7772/ZyyxDSS/GetTBDACCOUNTBOOKL',"财务数据中心",'erp_jd_ods_fact_accountbookl_cwzx')
# s1_funcB("http://10.225.137.124:7772/ZyyxDSS/GetTGLVOUCHERENTRY", 'erp_jd_ods_dim_voucherentry_cwzx')











# s1_funcB("http://10.225.137.124:7772/ZyyxDSS/GetTGLBALANCE",  'erp_jd_ods_dim_balance_cwzx')
# s1_funcB("http://10.225.137.124:7772/ZyyxDSS/GetFuKuanSQD",  'erp_jd_ods_dim_prepayment_cwzx')

# func_sjzx('http://10.225.137.124:7772/ZyyxDSS/GetTBASASSISTANTDATAENTRY',"财务数据中心",'erp_jd_ods_fact_assistantdataentry_cwzx')
# func_sjzx('http://10.225.137.124:7772/ZyyxDSS/GetTMETALOOKUPCLASS',"财务数据中心",'erp_jd_ods_fact_lookupclass_cwzx')
# func_sjzx('http://10.225.137.124:7772/ZyyxDSS/GetTBDFLEXITEMPROPERTY',"财务数据中心",'erp_jd_ods_fact_flexitemproperty_cwzx')