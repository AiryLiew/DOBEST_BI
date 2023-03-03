# -*- coding: utf-8 -*-
# 测试环境: python3.9.6

import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

from api_jd import func
from api_cwzx_append import s_funcB,s1_funcB

# s1_funcB("http://10.225.137.124:7772/ZyyxDSS/GetTGLBALANCE",  'erp_jd_ods_dim_balance_cwzx')
# s1_funcB("http://10.225.137.124:7772/ZyyxDSS/GetFuKuanSQD",  'erp_jd_ods_dim_prepayment_cwzx')


# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetQiTaRKD",    "杭州游卡文化创意有限公司",'erp_jd_ods_dim_othersreceiving_wc_cwzx'  )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetPanKuiD",    "杭州游卡文化创意有限公司",'erp_jd_ods_dim_inventoryloss_wc_cwzx'    )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetPanYingD",   "杭州游卡文化创意有限公司",'erp_jd_ods_dim_inventoryprofit_wc_cwzx'  )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouRKD",  "杭州游卡文化创意有限公司",'erp_jd_ods_dim_purchasereceiving_wc_cwzx')
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetJiShouJSD",  "杭州游卡文化创意有限公司",'erp_jd_ods_dim_consignment_wc_cwzx'      )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouTLD",  "杭州游卡文化创意有限公司",'erp_jd_ods_dim_purchasereturn_wc_cwzx'   )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouDD", "杭州游卡文化创意有限公司",'erp_jd_ods_dim_saleorders_wc_cwzx'       )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouTHD","杭州游卡文化创意有限公司",'erp_jd_ods_dim_salereturn_wc_cwzx'       )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouCKD","杭州游卡文化创意有限公司",'erp_jd_ods_dim_saleshipping_wc_cwzx'     )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetZuZhuangCX", "杭州游卡文化创意有限公司",'erp_jd_ods_dim_assemble_wc_cwzx'         )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDRD",  "杭州游卡文化创意有限公司",'erp_jd_ods_dim_distributedin_wc_cwzx'    )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDCD",  "杭州游卡文化创意有限公司",'erp_jd_ods_dim_distributedout_wc_cwzx'   )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetZhiJieDBD",  "杭州游卡文化创意有限公司",'erp_jd_ods_dim_allocation_wc_cwzx'       )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetYingFuD",    "杭州游卡文化创意有限公司",'erp_jd_ods_dim_voucherpayable_wc_cwzx'   )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetTGLVOUCHER", "杭州游卡文化创意有限公司",'erp_jd_ods_dim_voucher_wc_cwzx'          )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetWuLiaoSFHZ", "杭州游卡文化创意有限公司",'erp_jd_ods_dim_monthendinventory_wc_cwzx')


# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetQiTaRKD",    "杭州泳淳网络技术有限公司",'erp_jd_ods_dim_othersreceiving_yc_cwzx'  )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetPanKuiD",    "杭州泳淳网络技术有限公司",'erp_jd_ods_dim_inventoryloss_yc_cwzx'    )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetPanYingD",   "杭州泳淳网络技术有限公司",'erp_jd_ods_dim_inventoryprofit_yc_cwzx'  )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouRKD",  "杭州泳淳网络技术有限公司",'erp_jd_ods_dim_purchasereceiving_yc_cwzx')
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetJiShouJSD",  "杭州泳淳网络技术有限公司",'erp_jd_ods_dim_consignment_yc_cwzx'      )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouTLD",  "杭州泳淳网络技术有限公司",'erp_jd_ods_dim_purchasereturn_yc_cwzx'   )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouDD", "杭州泳淳网络技术有限公司",'erp_jd_ods_dim_saleorders_yc_cwzx'       )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouTHD","杭州泳淳网络技术有限公司",'erp_jd_ods_dim_salereturn_yc_cwzx'       )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouCKD","杭州泳淳网络技术有限公司",'erp_jd_ods_dim_saleshipping_yc_cwzx'     )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetZuZhuangCX", "杭州泳淳网络技术有限公司",'erp_jd_ods_dim_assemble_yc_cwzx'         )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDRD",  "杭州泳淳网络技术有限公司",'erp_jd_ods_dim_distributedin_yc_cwzx'    )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDCD",  "杭州泳淳网络技术有限公司",'erp_jd_ods_dim_distributedout_yc_cwzx'   )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetZhiJieDBD",  "杭州泳淳网络技术有限公司",'erp_jd_ods_dim_allocation_yc_cwzx'       )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetYingFuD",    "杭州泳淳网络技术有限公司",'erp_jd_ods_dim_voucherpayable_yc_cwzx'   )
s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetTGLVOUCHER", "杭州泳淳网络技术有限公司",'erp_jd_ods_dim_voucher_yc_cwzx'          ) 
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetWuLiaoSFHZ", "杭州泳淳网络技术有限公司",'erp_jd_ods_dim_monthendinventory_yc_cwzx') 

# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetQiTaRKD",    "杭州迷思文化创意有限公司",'erp_jd_ods_dim_othersreceiving_ms_cwzx'  )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetPanKuiD",    "杭州迷思文化创意有限公司",'erp_jd_ods_dim_inventoryloss_ms_cwzx'    )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetPanYingD",   "杭州迷思文化创意有限公司",'erp_jd_ods_dim_inventoryprofit_ms_cwzx'  )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouRKD",  "杭州迷思文化创意有限公司",'erp_jd_ods_dim_purchasereceiving_ms_cwzx')
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetJiShouJSD",  "杭州迷思文化创意有限公司",'erp_jd_ods_dim_consignment_ms_cwzx'      )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouTLD",  "杭州迷思文化创意有限公司",'erp_jd_ods_dim_purchasereturn_ms_cwzx'   )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouDD", "杭州迷思文化创意有限公司",'erp_jd_ods_dim_saleorders_ms_cwzx'       )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouTHD","杭州迷思文化创意有限公司",'erp_jd_ods_dim_salereturn_ms_cwzx'       )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouCKD","杭州迷思文化创意有限公司",'erp_jd_ods_dim_saleshipping_ms_cwzx'     )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetZuZhuangCX", "杭州迷思文化创意有限公司",'erp_jd_ods_dim_assemble_ms_cwzx'         )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDRD",  "杭州迷思文化创意有限公司",'erp_jd_ods_dim_distributedin_ms_cwzx'    )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDCD",  "杭州迷思文化创意有限公司",'erp_jd_ods_dim_distributedout_ms_cwzx'   )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetZhiJieDBD",  "杭州迷思文化创意有限公司",'erp_jd_ods_dim_allocation_ms_cwzx'       )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetYingFuD",    "杭州迷思文化创意有限公司",'erp_jd_ods_dim_voucherpayable_ms_cwzx'   )
s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetTGLVOUCHER", "杭州迷思文化创意有限公司",'erp_jd_ods_dim_voucher_ms_cwzx'          )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetWuLiaoSFHZ", "杭州迷思文化创意有限公司",'erp_jd_ods_dim_monthendinventory_ms_cwzx')

# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetQiTaRKD",    "上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_othersreceiving_kyk_cwzx'  )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetPanKuiD",    "上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_inventoryloss_kyk_cwzx'    )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetPanYingD",   "上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_inventoryprofit_kyk_cwzx'  )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouRKD",  "上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_purchasereceiving_kyk_cwzx')
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetJiShouJSD",  "上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_consignment_kyk_cwzx'      )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouTLD",  "上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_purchasereturn_kyk_cwzx'   )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouDD", "上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_saleorders_kyk_cwzx'       )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouTHD","上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_salereturn_kyk_cwzx'       )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouCKD","上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_saleshipping_kyk_cwzx'     )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetZuZhuangCX", "上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_assemble_kyk_cwzx'         )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDRD",  "上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_distributedin_kyk_cwzx'    )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDCD",  "上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_distributedout_kyk_cwzx'   )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetZhiJieDBD",  "上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_allocation_kyk_cwzx'       )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetYingFuD",    "上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_voucherpayable_kyk_cwzx'   )
s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetTGLVOUCHER", "上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_voucher_kyk_cwzx'          )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetWuLiaoSFHZ", "上海卡丫卡文化传播有限公司",'erp_jd_ods_dim_monthendinventory_kyk_cwzx')



# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetQiTaRKD",    "上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_othersreceiving_kyok_cwzx'  )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetPanKuiD",    "上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_inventoryloss_kyok_cwzx'    )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetPanYingD",   "上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_inventoryprofit_kyok_cwzx'  )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouRKD",  "上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_purchasereceiving_kyok_cwzx')
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetJiShouJSD",  "上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_consignment_kyok_cwzx'      )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouTLD",  "上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_purchasereturn_kyok_cwzx'   )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouDD", "上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_saleorders_kyok_cwzx'       )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouTHD","上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_salereturn_kyok_cwzx'       )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouCKD","上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_saleshipping_kyok_cwzx'     )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetZuZhuangCX", "上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_assemble_kyok_cwzx'         )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDRD",  "上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_distributedin_kyok_cwzx'    )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDCD",  "上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_distributedout_kyok_cwzx'   )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetZhiJieDBD",  "上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_allocation_kyok_cwzx'       )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetYingFuD",    "上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_voucherpayable_kyok_cwzx'   )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetTGLVOUCHER", "上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_voucher_kyok_cwzx'          )
# s_funcB("http://10.225.137.124:7772/ZyyxDSS/GetWuLiaoSFHZ", "上海卡哟卡网络技术有限公司",'erp_jd_ods_dim_monthendinventory_kyok_cwzx')







# 暂弃用
# func("http://10.225.137.124:7772/ZyyxDSS/GetPanKuiD",    "财务数据中心",'erp_jd_ods_dim_inventoryloss_wc_cwzx',    'erp_jd_ods_dim_inventoryloss_yc_cwzx',    'erp_jd_ods_dim_inventoryloss_ms_cwzx',    'erp_jd_ods_dim_inventoryloss_kyk_cwzx'    )
# func("http://10.225.137.124:7772/ZyyxDSS/GetQiTaRKD",    "财务数据中心",'erp_jd_ods_dim_othersreceiving_wc_cwzx',  'erp_jd_ods_dim_othersreceiving_yc_cwzx',  'erp_jd_ods_dim_othersreceiving_ms_cwzx',  'erp_jd_ods_dim_othersreceiving_kyk_cwzx'  )
# func("http://10.225.137.124:7772/ZyyxDSS/GetPanYingD",   "财务数据中心",'erp_jd_ods_dim_inventoryprofit_wc_cwzx',  'erp_jd_ods_dim_inventoryprofit_yc_cwzx',  'erp_jd_ods_dim_inventoryprofit_ms_cwzx' ,  'erp_jd_ods_dim_inventoryprofit_kyk_cwzx' )
# func("http://10.225.137.124:7772/ZyyxDSS/GetJiShouJSD",  "财务数据中心",'erp_jd_ods_dim_consignment_wc_cwzx',      'erp_jd_ods_dim_consignment_yc_cwzx',      'erp_jd_ods_dim_consignment_ms_cwzx' ,      'erp_jd_ods_dim_consignment_kyk_cwzx'     )
# func("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouTLD",  "财务数据中心",'erp_jd_ods_dim_purchasereturn_wc_cwzx',   'erp_jd_ods_dim_purchasereturn_yc_cwzx',   'erp_jd_ods_dim_purchasereturn_ms_cwzx',   'erp_jd_ods_dim_purchasereturn_kyk_cwzx'   )
# func("http://10.225.137.124:7772/ZyyxDSS/GetCaiGouRKD",  "财务数据中心",'erp_jd_ods_dim_purchasereceiving_wc_cwzx','erp_jd_ods_dim_purchasereceiving_yc_cwzx','erp_jd_ods_dim_purchasereceiving_ms_cwzx','erp_jd_ods_dim_purchasereceiving_kyk_cwzx')
# func("http://10.225.137.124:7772/ZyyxDSS/GetWuLiaoSFHZ", "财务数据中心",'erp_jd_ods_dim_monthendinventory_wc_cwzx','erp_jd_ods_dim_monthendinventory_yc_cwzx','erp_jd_ods_dim_monthendinventory_ms_cwzx',    'erp_jd_ods_dim_monthendinventory_kyk_cwzx')
# func("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouTHD","财务数据中心",'erp_jd_ods_dim_salereturn_wc_cwzx',       'erp_jd_ods_dim_salereturn_yc_cwzx',       'erp_jd_ods_dim_salereturn_ms_cwzx'  ,       'erp_jd_ods_dim_salereturn_kyk_cwzx'      )
# func("http://10.225.137.124:7772/ZyyxDSS/GetXiaoShouCKD","财务数据中心",'erp_jd_ods_dim_saleshipping_wc_cwzx',     'erp_jd_ods_dim_saleshipping_yc_cwzx',     'erp_jd_ods_dim_saleshipping_ms_cwzx' ,     'erp_jd_ods_dim_saleshipping_kyk_cwzx'    )
# func("http://10.225.137.124:7772/ZyyxDSS/GetZuZhuangCX", "财务数据中心",'erp_jd_ods_dim_assemble_wc_cwzx',         'erp_jd_ods_dim_assemble_yc_cwzx',         'erp_jd_ods_dim_assemble_ms_cwzx' ,         'erp_jd_ods_dim_assemble_kyk_cwzx'         )
# func("http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDRD",  "财务数据中心",'erp_jd_ods_dim_distributedin_wc_cwzx',    'erp_jd_ods_dim_distributedin_yc_cwzx',    'erp_jd_ods_dim_distributedin_ms_cwzx' ,    'erp_jd_ods_dim_distributedin_kyk_cwzx'   )
# func("http://10.225.137.124:7772/ZyyxDSS/GetFenBuSDCD",  "财务数据中心",'erp_jd_ods_dim_distributedout_wc_cwzx',   'erp_jd_ods_dim_distributedout_yc_cwzx',   'erp_jd_ods_dim_distributedout_ms_cwzx' ,   'erp_jd_ods_dim_distributedout_kyk_cwzx'  )
# func("http://10.225.137.124:7772/ZyyxDSS/GetZhiJieDBD",  "财务数据中心",'erp_jd_ods_dim_allocation_wc_cwzx',       'erp_jd_ods_dim_allocation_yc_cwzx',       'erp_jd_ods_dim_allocation_ms_cwzx'  ,       'erp_jd_ods_dim_allocation_kyk_cwzx'     )
# func("http://10.225.137.124:7772/ZyyxDSS/GetTGLVOUCHER", "财务数据中心",'erp_jd_ods_dim_voucher_wc_cwzx',        'erp_jd_ods_dim_voucher_yc_cwzx',          'erp_jd_ods_dim_voucher_ms_cwzx',          'erp_jd_ods_dim_voucher_kyk_cwzx' )
