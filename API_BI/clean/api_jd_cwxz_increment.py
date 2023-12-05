# -*- coding: utf-8 -*-
# 测试环境: python3.9.6

import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

from api_cwzx_append import a_func,a1_func,s1_funcB,onceback
from api_jd import func,func_QTCK,func_wjg, func_sjzx


company = { "杭州游卡文化创意有限公司":'wc',
            "杭州泳淳网络技术有限公司":'yc',
            "杭州迷思文化创意有限公司":'ms',
            "上海卡丫卡文化传播有限公司":'kyk',
            "上海卡哟卡网络技术有限公司":'kyok',
            "杭州游卡文化创意有限公司拱墅区分公司":'wc01'
}

# 增量接口
dict_a_func = { "http://10.225.137.124:7772/ZyyxDSS/GetQiTaRKD":    ['erp_jd_ods_dim_othersreceiving'  ,'fid'],
                "http://10.225.137.124:7772/ZyyxDSS/GetPanKuiD":    ['erp_jd_ods_dim_inventoryloss'    ,'fid'],
                "http://10.225.137.124:7772/ZyyxDSS/GetPanYingD":   ['erp_jd_ods_dim_inventoryprofit'  ,'fid'],
                "http://10.225.137.124:7772/ZyyxDSS/GetCaiGouRKD":  ['erp_jd_ods_dim_purchasereceiving','fid'],
                "http://10.225.137.124:7772/ZyyxDSS/GetJiShouJSD":  ['erp_jd_ods_dim_consignment'      ,'fid'],
                "http://10.225.137.124:7772/ZyyxDSS/GetCaiGouTLD":  ['erp_jd_ods_dim_purchasereturn'   ,'fid'],
                "http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouTHD":['erp_jd_ods_dim_salereturn'       ,'fid'],
                "http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouCKD":['erp_jd_ods_dim_saleshipping'     ,'fid'],
                "http://10.225.137.124:7772/ZyyxDSS/GetZuZhuangCX": ['erp_jd_ods_dim_assemble'         ,'fid'],
                "http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDRD":  ['erp_jd_ods_dim_distributedin'    ,'fid'],
                "http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDCD":  ['erp_jd_ods_dim_distributedout'   ,'fid'],
                "http://10.225.137.124:7772/ZyyxDSS/GetZhiJieDBD":  ['erp_jd_ods_dim_allocation'       ,'fid'],
                "http://10.225.137.124:7772/ZyyxDSS/GetTGLVOUCHER": ['erp_jd_ods_dim_voucher'          ,'fVoucherID']                   
}


dict_a1_func = {"http://10.225.137.124:7772/ZyyxDSS/GetTGLACCTAGEBALANCE":     ['erp_jd_ods_dim_acctagebalance_cwzx',         'fdetailid'],
                "http://10.225.137.124:7772/ZyyxDSS/GetFuKuanSQD":             ['erp_jd_ods_dim_prepayment_cwzx',             'fid'],
                "http://10.225.137.124:7772/ZyyxDSS/GetTHSSTOCKDIMENSION":     ['erp_jd_ods_dim_ths_stockdimension_cwzx',     'fentryid'],
                "http://10.225.137.124:7772/ZyyxDSS/GetTHSBALANCEH":           ['erp_jd_ods_dim_ths_balanceh_cwzx',           'fid'] ,
                "http://10.225.137.124:7772/ZyyxDSS/GetTHSINIVBALANCEH":       ['erp_jd_ods_dim_ths_inivbalanceh_cwzx',       'fid'],
                "http://10.225.137.124:7772/ZyyxDSS/GetTHSADJUSTMENTBILLENTRY":['erp_jd_ods_dim_ths_adjustmentbillentry_cwzx','fid'] ,
                "http://10.225.137.124:7772/ZyyxDSS/GetTHSADJUSTMENTBILL":     ['erp_jd_ods_dim_ths_adjustmentbill_cwzx',     'fid'] 
}




# 全量接口

dict_onceback = {"http://10.225.137.124:7772/ZyyxDSS/GetTBDMATERIALGROUP": 'erp_jd_ods_dim_materialgroup_cwzx',
                "http://10.225.137.124:7772/ZyyxDSS/GetTBDMATERIALGROUPL":'erp_jd_ods_dim_materialgroupl_cwzx',
                "http://10.225.137.124:7772/ZyyxDSS/GetTHSOUTACCTG":      'erp_jd_ods_dim_outacctg_cwzx',
                "http://10.225.137.124:7772/ZyyxDSS/GetTHSCALDIMENSIONS": 'erp_jd_ods_dim_caldimensions_cwzx',
                "http://10.225.137.124:7772/ZyyxDSS/GetTBDSTOCK":         'erp_jd_ods_dim_tbd_stock_cwzx',
                "http://10.225.137.124:7772/ZyyxDSS/GetTBDSTOCKL":        'erp_jd_ods_dim_tbd_stockl_cwzx' 
}


dict_s1_funcB = {"http://10.225.137.124:7772/ZyyxDSS/GetTGLVOUCHERENTRY":      'erp_jd_ods_dim_voucherentry_cwzx',
                 "http://10.225.137.124:7772/ZyyxDSS/GetTGLBALANCE":           'erp_jd_ods_dim_balance_cwzx',
                 "http://10.225.137.124:7772/ZyyxDSS/GetTHSINIVBALANCE":       'erp_jd_ods_dim_ths_inivbalance_cwzx',
                 "http://10.225.137.124:7772/ZyyxDSS/GetTHSBALANCE":           'erp_jd_ods_dim_ths_balance_cwzx',
                 "http://10.225.137.124:7772/ZyyxDSS/GetTHSINIVSTOCKDIMENSION":'erp_jd_ods_dim_ths_inivstockdimension_cwzx'
}


dict_func = {   "http://10.225.137.124:7772/ZyyxDSS/GetKeHu":      'erp_jd_ods_fact_client',
                "http://10.225.137.124:7772/ZyyxDSS/GetWuLiao":    'erp_jd_ods_fact_classify',
                "http://10.225.137.124:7772/ZyyxDSS/GetCaiGouDD":  'erp_jd_ods_dim_purchaseorders',
                "http://10.225.137.124:7772/ZyyxDSS/GetYingFuD":   'erp_jd_ods_dim_voucherpayable',
                "http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouDD":'erp_jd_ods_dim_saleorders',
                "http://10.225.137.124:7772/ZyyxDSS/GetShouKuanD": 'erp_jd_ods_dim_proceeds',
                "http://10.225.137.124:7772/ZyyxDSS/GetTBDACCOUNT":'erp_jd_ods_fact_account'
}


dict_func_wjg = {   "http://10.225.137.124:7772/ZyyxDSS/GetTBDFLEXITEMDETAILV":'erp_jd_ods_fact_flexitemdetailv_cwzx',
                    "http://10.225.137.124:7772/ZyyxDSS/GetTBDACCOUNTL":       'erp_jd_ods_fact_accountl_cwzx',
                    "http://10.225.137.124:7772/ZyyxDSS/GetTBDACCOUNTBOOKL":   'erp_jd_ods_fact_accountbookl_cwzx'
}




func_QTCK("财务数据中心",'erp_jd_ods_dim_othersshipping_wc_cwzx',   'erp_jd_ods_dim_othersshipping_yc_cwzx',   'erp_jd_ods_dim_othersshipping_ms_cwzx'  ,'erp_jd_ods_dim_othersshipping_kyk_cwzx','erp_jd_ods_dim_othersshipping_kyok_cwzx' ,'erp_jd_ods_dim_othersshipping_wc01_cwzx')

# s1_funcB("http://10.225.137.124:7772/ZyyxDSS/GetFuKuanSQD",  'erp_jd_ods_dim_prepayment_cwzx')

# func_sjzx('http://10.225.137.124:7772/ZyyxDSS/GetTBASASSISTANTDATAENTRY',"财务数据中心",'erp_jd_ods_fact_assistantdataentry_cwzx')
# func_sjzx('http://10.225.137.124:7772/ZyyxDSS/GetTMETALOOKUPCLASS',"财务数据中心",'erp_jd_ods_fact_lookupclass_cwzx')
# func_sjzx('http://10.225.137.124:7772/ZyyxDSS/GetTBDFLEXITEMPROPERTY',"财务数据中心",'erp_jd_ods_fact_flexitemproperty_cwzx')


for k, v in company.items():
    for n, m in dict_a_func.items():  
        a_func(n, k, m[0] + '_' + v + '_cwzx', m[1])


for n,m in dict_a1_func.items():  
    a1_func(n,m[0],m[1])


for n,m in dict_onceback.items():  
    onceback(n,m)  


for n,m in dict_s1_funcB.items():  
    s1_funcB(n,m)


for n,m in dict_func.items():  
    func(n,"财务数据中心",m+'_wc_cwzx', m+'_yc_cwzx',m+'_ms_cwzx',m+'_kyk_cwzx' ,m+'_kyok_cwzx',m+'_wc01_cwzx')



for n,m in dict_func_wjg.items():  
    func_wjg(n,"财务数据中心",m)
