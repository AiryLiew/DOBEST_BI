# -*- coding: utf-8 -*-
# 测试环境: python3.9.6

import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

import time
import numpy as np  
from api_cwzx_append import s1_funcB
from sqlalchemy import create_engine,text
from datetime import datetime,timedelta




dict_s1_funcB = {"http://10.225.137.124:7772/ZyyxDSS/GetTHSINIVBALANCE":       'erp_jd_ods_dim_ths_inivbalance_cwzx'
}


for n,m in dict_s1_funcB.items():  
    print(m)
    s1_funcB(n,m)



engine.dispose()
