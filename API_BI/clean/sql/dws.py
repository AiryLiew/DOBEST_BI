# -*- coding: utf-8 -*-
# 测试环境: python3.9.6

# day refresh 19:00:00
import sys
sys.path.append(r'C:\Users\liujin02\Desktop\BI建设\API_BI\moudle')

from key_tab import sqlrun
import os
from datetime import datetime
print("\n","START DWS", datetime.now(),"\n")

  
folder_path = r'C:\Users\liujin02\Desktop\BI建设\API_BI\clean\sql\dws' 

def run(folder_path):
    files = [os.path.join(folder_path, f) for f in os.listdir(folder_path)]  

    for file_path in files:  
        sqlrun(file_path) 


run(folder_path)

