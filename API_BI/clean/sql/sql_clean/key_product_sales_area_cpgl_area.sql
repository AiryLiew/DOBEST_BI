drop table if exists erp_jd_ads.`key_product_sales_area_cpgl_area`;
CREATE TABLE erp_jd_ads.`key_product_sales_area_cpgl_area` (
SELECT a.产品大类,
a.`部门`,
a.`业务区域`,
sum(case when a.周分类='本周' then a.`总销量` end) 本周销量,
sum(case when a.周分类='上周' then a.`总销量` end) 上周销量,
round(sum(case when a.周分类='本周' then a.`总销售额` end),2) 本周销售额,
round(sum(case when a.周分类='上周' then a.`总销售额` end),2) 上周销售额,
round(sum(case when a.周分类='本周' then a.`毛利` end),2) 本周毛利,
round(sum(case when a.周分类='上周' then a.`毛利` end),2) 上周毛利,
round(sum(case when a.周分类='本周' then a.`毛利` end)/sum(case when a.周分类='上周' then a.`毛利` end)-1,3) 周毛利环比,
round(sum(case when a.周分类='本周' then a.`总销量` end)/sum(case when a.周分类='上周' then a.`总销量` end)-1,3) 周销量环比,
round(sum(case when a.周分类='本周' then a.`总销售额` end)/sum(case when a.周分类='上周' then a.`总销售额` end)-1,3) 周销售额环比,

sum(case when a.月分类='本月' then a.`总销量` end) 本月销量,
sum(case when a.月分类='上月同期' then a.`总销量` end) 上月同期销量,
sum(case when a.月分类='去年同期/本月' then a.`总销量` end) 去年同期月销量,
sum(case when a.月分类='上月' then a.`总销量` end) 上月销量,
sum(case when a.月分类='上月环比' then a.`总销量` end) 上月环比销量,
sum(case when a.月分类='去年同期/上月' then a.`总销量` end) 去年同期上月销量,

round(sum(case when a.月分类='本月' then a.`总销售额` end),2) 本月销售额,
round(sum(case when a.月分类='上月同期' then a.`总销售额` end),2) 上月同期销售额,
round(sum(case when a.月分类='去年同期/本月' then a.`总销售额` end),2) 去年同期月销售额,
round(sum(case when a.月分类='上月' then a.`总销售额` end),2) 上月销售额,
round(sum(case when a.月分类='上月环比' then a.`总销售额` end),2) 上月环比销售额,
round(sum(case when a.月分类='去年同期/上月' then a.`总销售额` end),2) 去年同期上月销售额,

round(sum(case when a.月分类='本月' then a.`毛利` end),2) 本月毛利,
round(sum(case when a.月分类='上月同期' then a.`毛利` end),2) 上月同期毛利,
round(sum(case when a.月分类='去年同期/本月' then a.`毛利` end),2) 去年同期月毛利,
round(sum(case when a.月分类='上月' then a.`毛利` end),2) 上月毛利,
round(sum(case when a.月分类='上月环比' then a.`毛利` end),2) 上月环比毛利,
round(sum(case when a.月分类='去年同期/上月' then a.`毛利` end),2) 去年同期上月毛利,

round(sum(case when a.月分类='本月' then a.`毛利` end)/sum(case when a.月分类='上月同期' then a.`毛利` end)-1,3) 本月毛利环比,
round(sum(case when a.月分类='本月' then a.`总销量` end)/sum(case when a.月分类='上月同期' then a.`总销量` end)-1,3) 本月销量环比,
round(sum(case when a.月分类='本月' then a.`总销售额` end)/sum(case when a.月分类='上月同期' then a.`总销售额` end)-1,3) 本月销售额环比,
round(sum(case when a.月分类='本月' then a.`毛利` end)/sum(case when a.月分类='去年同期/本月' then a.`毛利` end)-1,3) 本月毛利同比,
round(sum(case when a.月分类='本月' then a.`总销量` end)/sum(case when a.月分类='去年同期/本月' then a.`总销量` end)-1,3) 本月销量同比,
round(sum(case when a.月分类='本月' then a.`总销售额` end)/sum(case when a.月分类='去年同期/本月' then a.`总销售额` end)-1,3) 本月销售额同比,

round(sum(case when a.月分类='上月' then a.`毛利` end)/sum(case when a.月分类='上月环比' then a.`毛利` end)-1,3) 上月毛利环比,
round(sum(case when a.月分类='上月' then a.`总销量` end)/sum(case when a.月分类='上月环比' then a.`总销量` end)-1,3) 上月销量环比,
round(sum(case when a.月分类='上月' then a.`总销售额` end)/sum(case when a.月分类='上月环比' then a.`总销售额` end)-1,3) 上月销售额环比,
round(sum(case when a.月分类='上月' then a.`毛利` end)/sum(case when a.月分类='去年同期/上月' then a.`毛利` end)-1,3) 上月毛利同比,
round(sum(case when a.月分类='上月' then a.`总销量` end)/sum(case when a.月分类='去年同期/上月' then a.`总销量` end)-1,3) 上月销量同比,
round(sum(case when a.月分类='上月' then a.`总销售额` end)/sum(case when a.月分类='去年同期/上月' then a.`总销售额` end)-1,3) 上月销售额同比,


sum(case when a.季分类='上季度' then a.`总销量` end) 上季度销量,
sum(case when a.季分类='上季度环比' then a.`总销量` end) 上季度环比销量,
sum(case when a.季分类='上季度同比' then a.`总销量` end) 上季度同期销量,
round(sum(case when a.季分类='上季度' then a.`总销售额` end),2) 上季度销售额,
round(sum(case when a.季分类='上季度环比' then a.`总销售额` end),2) 上季度环比销售额,
round(sum(case when a.季分类='上季度同比' then a.`总销售额` end),2) 上季度同期销售额,
round(sum(case when a.季分类='上季度' then a.`毛利` end),2) 上季度毛利,
round(sum(case when a.季分类='上季度环比' then a.`毛利` end),2) 上季度环比毛利,
round(sum(case when a.季分类='上季度同比' then a.`毛利` end),2) 上季度同期毛利,

round(sum(case when a.季分类='上季度' then a.`毛利` end)/sum(case when a.季分类='上季度环比' then a.`毛利` end)-1,3) 上季度毛利环比,
round(sum(case when a.季分类='上季度' then a.`总销量` end)/sum(case when a.季分类='上季度环比' then a.`总销量` end)-1,3) 上季度销量环比,
round(sum(case when a.季分类='上季度' then a.`总销售额` end)/sum(case when a.季分类='上季度环比' then a.`总销售额` end)-1,3) 上季度销售额环比,
round(sum(case when a.季分类='上季度' then a.`毛利` end)/sum(case when a.季分类='上季度同比' then a.`毛利` end)-1,3) 上季度毛利同比,
round(sum(case when a.季分类='上季度' then a.`总销量` end)/sum(case when a.季分类='上季度同比' then a.`总销量` end)-1,3) 上季度销量同比,
round(sum(case when a.季分类='上季度' then a.`总销售额` end)/sum(case when a.季分类='上季度同比' then a.`总销售额` end)-1,3) 上季度销售额同比,
c.本周标准销售订单,
c.本周活动返销售订单,
c.本周年返销售订单,
c.本周寄售销售订单 ,
c.本月标准销售订单,
c.本月活动返销售订单,
c.本月年返销售订单,
c.本月寄售销售订单 ,
c.上月标准销售订单,
c.上月活动返销售订单,
c.上月年返销售订单,
c.上月寄售销售订单 
from(
    SELECT `部门`,
    `业务区域`,
    `客户`,
    `产品大类`,
    `产品中类`,
    `产品小类`,
    `产品名称`,
    `年` ,
    `季度` ,
    `月`,
    `周`,
    `日期`,
    `总销量`,
    `总销售额`,
    `毛利`,
    yearweek(日期,1) 年周,
    case when yearweek(日期,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 7 day),1) then '本周' 
        when yearweek(日期,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 14 day),1) then '上周'
        else '其他' end 周分类,
    case when 月=month(CURRENT_DATE()) and 年=year(CURRENT_DATE())  then '本月' 
        when 月=month(CURRENT_DATE()) and 年=year(CURRENT_DATE())-1 and day(日期)<day(CURRENT_DATE()) then '去年同期/本月' 
        when (月=month(CURRENT_DATE())-1 and 年=year(CURRENT_DATE()) and day(日期)<day(CURRENT_DATE()) and month(CURRENT_DATE())<>1) or (月=12 and 年=year(CURRENT_DATE())-1 and day(日期)<day(CURRENT_DATE()) and month(CURRENT_DATE())=1) then '上月同期'
        when (月=month(CURRENT_DATE())-1 and 年=year(CURRENT_DATE()) and month(CURRENT_DATE())<>1) or  (月=12 and 年=year(CURRENT_DATE())-1 and month(CURRENT_DATE())=1)  then '上月'
        when (月=month(CURRENT_DATE())-2 and 年=year(CURRENT_DATE()) and month(CURRENT_DATE())>2) or  (月=11 and 年=year(CURRENT_DATE())-1 and month(CURRENT_DATE())=1)  or  (月=12 and 年=year(CURRENT_DATE())-1 and month(CURRENT_DATE())=2) then '上月环比'
        when (月=month(CURRENT_DATE())-1 and 年=year(CURRENT_DATE())-1 and month(CURRENT_DATE())<>1) or  (月=12 and 年=year(CURRENT_DATE())-2 and month(CURRENT_DATE())=1)  then '去年同期/上月'
        else '其他' end 月分类,
    case when (季度=quarter(CURRENT_DATE())-1 and quarter(CURRENT_DATE())<>1 and 年=year(CURRENT_DATE())) or (季度=4 and quarter(CURRENT_DATE())=1 and 年=year(CURRENT_DATE())-1)  then '上季度' 
        when (季度=quarter(CURRENT_DATE())-1 and quarter(CURRENT_DATE()) >2 and 年=year(CURRENT_DATE())) or (季度=3 and quarter(CURRENT_DATE())=1 and 年=year(CURRENT_DATE())-1)  or (季度=4 and quarter(CURRENT_DATE())=2 and 年=year(CURRENT_DATE())-1) then '上季度环比' 
        when 季度=quarter(CURRENT_DATE())-1  and 年=year(CURRENT_DATE())-1 then '上季度同比'
        else '其他' end 季分类
    FROM erp_jd_ads.key_product_sales_area_cpgl
    
    union all

    SELECT `部门`,
    `业务区域`,
    `客户`,
    case when `产品大类`= '推理桌游' then '推理桌游（不含自采）' else `产品大类` end 产品大类,
    `产品中类`,
    `产品小类`,
    `产品名称`,
    `年` ,
    `季度` ,
    `月`,
    `周`,
    `日期`,
    `总销量`,
    `总销售额`,
    `毛利`,
    yearweek(日期,1) 年周,
    case when yearweek(日期,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 7 day),1) then '本周' 
        when yearweek(日期,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 14 day),1) then '上周'
        else '其他' end 周分类,
    case when 月=month(CURRENT_DATE()) and 年=year(CURRENT_DATE())  then '本月' 
        when 月=month(CURRENT_DATE()) and 年=year(CURRENT_DATE())-1 and day(日期)<day(CURRENT_DATE()) then '去年同期/本月' 
        when (月=month(CURRENT_DATE())-1 and 年=year(CURRENT_DATE()) and day(日期)<day(CURRENT_DATE()) and month(CURRENT_DATE())<>1) or (月=12 and 年=year(CURRENT_DATE())-1 and day(日期)<day(CURRENT_DATE()) and month(CURRENT_DATE())=1) then '上月同期'
        when (月=month(CURRENT_DATE())-1 and 年=year(CURRENT_DATE()) and month(CURRENT_DATE())<>1) or  (月=12 and 年=year(CURRENT_DATE())-1 and month(CURRENT_DATE())=1)  then '上月'
        when (月=month(CURRENT_DATE())-2 and 年=year(CURRENT_DATE()) and month(CURRENT_DATE())>2) or  (月=11 and 年=year(CURRENT_DATE())-1 and month(CURRENT_DATE())=1)  or  (月=12 and 年=year(CURRENT_DATE())-1 and month(CURRENT_DATE())=2) then '上月环比'
        when (月=month(CURRENT_DATE())-1 and 年=year(CURRENT_DATE())-1 and month(CURRENT_DATE())<>1) or  (月=12 and 年=year(CURRENT_DATE())-2 and month(CURRENT_DATE())=1)  then '去年同期/上月'
        else '其他' end 月分类,
    case when (季度=quarter(CURRENT_DATE())-1 and quarter(CURRENT_DATE())<>1 and 年=year(CURRENT_DATE())) or (季度=4 and quarter(CURRENT_DATE())=1 and 年=year(CURRENT_DATE())-1)  then '上季度' 
        when (季度=quarter(CURRENT_DATE())-1 and quarter(CURRENT_DATE()) >2 and 年=year(CURRENT_DATE())) or (季度=3 and quarter(CURRENT_DATE())=1 and 年=year(CURRENT_DATE())-1)  or (季度=4 and quarter(CURRENT_DATE())=2 and 年=year(CURRENT_DATE())-1) then '上季度环比' 
        when 季度=quarter(CURRENT_DATE())-1  and 年=year(CURRENT_DATE())-1 then '上季度同比'
        else '其他' end 季分类
    FROM erp_jd_ads.key_product_sales_area_cpgl
    where `产品大类` = '推理桌游'
    and `产品中类` not in ('电商B端剧本杀','电商C端剧本杀') 

) a


left join(
    SELECT a.产品大类,
    a.客户,
    sum(case when a.danjulxmc = "标准销售订单" and a.周分类 = '本周' then a.xiaoshousl else 0 end) 本周标准销售订单,
    sum(case when a.danjulxmc = "活动返销售订单" and a.周分类 = '本周' then a.xiaoshousl  else 0 end) 本周活动返销售订单,
    sum(case when a.danjulxmc = "年返销售订单" and a.周分类 = '本周' then a.xiaoshousl  else 0 end) 本周年返销售订单,
    sum(case when a.danjulxmc = "寄售销售订单" and a.周分类 = '本周' then a.xiaoshousl  else 0 end) 本周寄售销售订单,
    sum(case when a.danjulxmc = "标准销售订单" and a.月分类 = '本月' then a.xiaoshousl else 0 end) 本月标准销售订单,
    sum(case when a.danjulxmc = "活动返销售订单" and a.月分类 = '本月' then a.xiaoshousl  else 0 end) 本月活动返销售订单,
    sum(case when a.danjulxmc = "年返销售订单" and a.月分类 = '本月' then a.xiaoshousl  else 0 end) 本月年返销售订单,
    sum(case when a.danjulxmc = "寄售销售订单" and a.月分类 = '本月' then a.xiaoshousl  else 0 end) 本月寄售销售订单,
    sum(case when a.danjulxmc = "标准销售订单" and a.月分类 = '上月' then a.xiaoshousl else 0 end) 上月标准销售订单,
    sum(case when a.danjulxmc = "活动返销售订单" and a.月分类 = '上月' then a.xiaoshousl  else 0 end) 上月活动返销售订单,
    sum(case when a.danjulxmc = "年返销售订单" and a.月分类 = '上月' then a.xiaoshousl  else 0 end) 上月年返销售订单,
    sum(case when a.danjulxmc = "寄售销售订单" and a.月分类 = '上月' then a.xiaoshousl  else 0 end) 上月寄售销售订单
    from(
        SELECT  b.classify 产品大类, 
        b.classify_1 产品中类, 
        b.classify_2 产品小类, 
        a.wuliaomc 产品名称,
        a.riqi,
        a.xiaoshousl ,
        a.danjulxmc,
        a.kehumc 客户,
        yearweek(a.riqi,1) 年周,
        case when yearweek(riqi,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 7 day),1) then '本周' 
            when yearweek(riqi,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 14 day),1) then '上周'
            else '其他' end 周分类,
        case when month(riqi)=month(CURRENT_DATE()) and year(riqi)=year(CURRENT_DATE())  then '本月' 
            when month(riqi)=month(CURRENT_DATE()) and year(riqi)=year(CURRENT_DATE())-1 and day(riqi)<day(CURRENT_DATE()) then '去年同期/本月' 
            when (month(riqi)=month(CURRENT_DATE())-1 and year(riqi)=year(CURRENT_DATE()) and day(riqi)<day(CURRENT_DATE()) and month(CURRENT_DATE())<>1) or (month(riqi)=12 and year(riqi)=year(CURRENT_DATE())-1 and day(riqi)<day(CURRENT_DATE()) and month(CURRENT_DATE())=1) then '上月同期'
            when (month(riqi)=month(CURRENT_DATE())-1 and year(riqi)=year(CURRENT_DATE()) and month(CURRENT_DATE())<>1) or  (month(riqi)=12 and year(riqi)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())=1)  then '上月'
            when (month(riqi)=month(CURRENT_DATE())-2 and year(riqi)=year(CURRENT_DATE()) and month(CURRENT_DATE())>2) or  (month(riqi)=11 and year(riqi)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())=1)  or  (month(riqi)=12 and year(riqi)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())=2) then '上月环比'
            when (month(riqi)=month(CURRENT_DATE())-1 and year(riqi)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())<>1) or  (month(riqi)=12 and year(riqi)=year(CURRENT_DATE())-2 and month(CURRENT_DATE())=1)  then '去年同期/上月'
            else '其他' end 月分类,
        case when (quarter(riqi)=quarter(CURRENT_DATE())-1 and quarter(CURRENT_DATE())<>1 and year(riqi)=year(CURRENT_DATE())) or (quarter(riqi)=4 and quarter(CURRENT_DATE())=1 and year(riqi)=year(CURRENT_DATE())-1)  then '上季度' 
            when (quarter(riqi)=quarter(CURRENT_DATE())-1 and quarter(CURRENT_DATE()) >2 and year(riqi)=year(CURRENT_DATE())) or (quarter(riqi)=3 and quarter(CURRENT_DATE())=1 and year(riqi)=year(CURRENT_DATE())-1)  or (quarter(riqi)=4 and quarter(CURRENT_DATE())=2 and year(riqi)=year(CURRENT_DATE())-1) then '上季度环比' 
            when quarter(riqi)=quarter(CURRENT_DATE())-1  and year(riqi)=year(CURRENT_DATE())-1 then '上季度同比'
            else '其他' end 季分类

        from erp_jd_dwd.erp_jd_dwd_dim_saleorders  a 
        LEFT JOIN(
            SELECT classify,classify_1,classify_2,wuliaomc 
            from erp_jd_dwd.erp_jd_dwd_fact_classify 
        ) b on a.wuliaomc = b.wuliaomc 
        where b.classify_1 not in ('海外系列','阵面对决','IP系列','自研B端剧本杀','其他','剧本杀配件','电商剧本杀道具','收藏卡') 
        and b.classify in ('欢乐坊','推理桌游','三国杀','Yokakids','周边')
        and a.wuliaomc not like '扑克三国杀%%'
        and a.wuliaomc not like '贵人鸟资源卡包第一弹%%'

        union all 

        SELECT  b.classify 产品大类, 
        b.classify_1 产品中类, 
        b.classify_2 产品小类, 
        a.wuliaomc 产品名称,
        a.riqi,
        a.xiaoshousl ,
        a.danjulxmc,
        a.kehumc 客户,
        yearweek(a.riqi,1) 年周,
        case when yearweek(riqi,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 7 day),1) then '本周' 
            when yearweek(riqi,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 14 day),1) then '上周'
            else '其他' end 周分类,
        case when month(riqi)=month(CURRENT_DATE()) and year(riqi)=year(CURRENT_DATE())  then '本月' 
            when month(riqi)=month(CURRENT_DATE()) and year(riqi)=year(CURRENT_DATE())-1 and day(riqi)<day(CURRENT_DATE()) then '去年同期/本月' 
            when (month(riqi)=month(CURRENT_DATE())-1 and year(riqi)=year(CURRENT_DATE()) and day(riqi)<day(CURRENT_DATE()) and month(CURRENT_DATE())<>1) or (month(riqi)=12 and year(riqi)=year(CURRENT_DATE())-1 and day(riqi)<day(CURRENT_DATE()) and month(CURRENT_DATE())=1) then '上月同期'
            when (month(riqi)=month(CURRENT_DATE())-1 and year(riqi)=year(CURRENT_DATE()) and month(CURRENT_DATE())<>1) or  (month(riqi)=12 and year(riqi)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())=1)  then '上月'
            when (month(riqi)=month(CURRENT_DATE())-2 and year(riqi)=year(CURRENT_DATE()) and month(CURRENT_DATE())>2) or  (month(riqi)=11 and year(riqi)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())=1)  or  (month(riqi)=12 and year(riqi)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())=2) then '上月环比'
            when (month(riqi)=month(CURRENT_DATE())-1 and year(riqi)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())<>1) or  (month(riqi)=12 and year(riqi)=year(CURRENT_DATE())-2 and month(CURRENT_DATE())=1)  then '去年同期/上月'
            else '其他' end 月分类,
        case when (quarter(riqi)=quarter(CURRENT_DATE())-1 and quarter(CURRENT_DATE())<>1 and year(riqi)=year(CURRENT_DATE())) or (quarter(riqi)=4 and quarter(CURRENT_DATE())=1 and year(riqi)=year(CURRENT_DATE())-1)  then '上季度' 
            when (quarter(riqi)=quarter(CURRENT_DATE())-1 and quarter(CURRENT_DATE()) >2 and year(riqi)=year(CURRENT_DATE())) or (quarter(riqi)=3 and quarter(CURRENT_DATE())=1 and year(riqi)=year(CURRENT_DATE())-1)  or (quarter(riqi)=4 and quarter(CURRENT_DATE())=2 and year(riqi)=year(CURRENT_DATE())-1) then '上季度环比' 
            when quarter(riqi)=quarter(CURRENT_DATE())-1  and year(riqi)=year(CURRENT_DATE())-1 then '上季度同比'
            else '其他' end 季分类
        from erp_jd_dwd.erp_jd_dwd_dim_saleorders  a 
        LEFT JOIN(
            SELECT classify,classify_1,classify_2,wuliaomc 
            from erp_jd_dwd.erp_jd_dwd_fact_classify 
        ) b on a.wuliaomc = b.wuliaomc 
        where b.classify_2 ='其他闪'


        union all 

        SELECT  b.classify 产品大类, 
        b.classify_1 产品中类, 
        b.classify_2 产品小类, 
        a.wuliaomc 产品名称,
        a.riqi,
        a.xiaoshousl ,
        a.danjulxmc,
        a.kehumc 客户,
        yearweek(a.riqi,1) 年周,
        case when yearweek(riqi,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 7 day),1) then '本周' 
            when yearweek(riqi,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 14 day),1) then '上周'
            else '其他' end 周分类,
        case when month(riqi)=month(CURRENT_DATE()) and year(riqi)=year(CURRENT_DATE())  then '本月' 
            when month(riqi)=month(CURRENT_DATE()) and year(riqi)=year(CURRENT_DATE())-1 and day(riqi)<day(CURRENT_DATE()) then '去年同期/本月' 
            when (month(riqi)=month(CURRENT_DATE())-1 and year(riqi)=year(CURRENT_DATE()) and day(riqi)<day(CURRENT_DATE()) and month(CURRENT_DATE())<>1) or (month(riqi)=12 and year(riqi)=year(CURRENT_DATE())-1 and day(riqi)<day(CURRENT_DATE()) and month(CURRENT_DATE())=1) then '上月同期'
            when (month(riqi)=month(CURRENT_DATE())-1 and year(riqi)=year(CURRENT_DATE()) and month(CURRENT_DATE())<>1) or  (month(riqi)=12 and year(riqi)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())=1)  then '上月'
            when (month(riqi)=month(CURRENT_DATE())-2 and year(riqi)=year(CURRENT_DATE()) and month(CURRENT_DATE())>2) or  (month(riqi)=11 and year(riqi)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())=1)  or  (month(riqi)=12 and year(riqi)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())=2) then '上月环比'
            when (month(riqi)=month(CURRENT_DATE())-1 and year(riqi)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())<>1) or  (month(riqi)=12 and year(riqi)=year(CURRENT_DATE())-2 and month(CURRENT_DATE())=1)  then '去年同期/上月'
            else '其他' end 月分类,
        case when (quarter(riqi)=quarter(CURRENT_DATE())-1 and quarter(CURRENT_DATE())<>1 and year(riqi)=year(CURRENT_DATE())) or (quarter(riqi)=4 and quarter(CURRENT_DATE())=1 and year(riqi)=year(CURRENT_DATE())-1)  then '上季度' 
            when (quarter(riqi)=quarter(CURRENT_DATE())-1 and quarter(CURRENT_DATE()) >2 and year(riqi)=year(CURRENT_DATE())) or (quarter(riqi)=3 and quarter(CURRENT_DATE())=1 and year(riqi)=year(CURRENT_DATE())-1)  or (quarter(riqi)=4 and quarter(CURRENT_DATE())=2 and year(riqi)=year(CURRENT_DATE())-1) then '上季度环比' 
            when quarter(riqi)=quarter(CURRENT_DATE())-1  and year(riqi)=year(CURRENT_DATE())-1 then '上季度同比'
            else '其他' end 季分类
        from erp_jd_dwd.erp_jd_dwd_dim_saleorders  a 
        LEFT JOIN(
            SELECT classify,classify_1,classify_2,wuliaomc 
            from erp_jd_dwd.erp_jd_dwd_fact_classify 
        ) b on a.wuliaomc = b.wuliaomc 
        where a.wuliaomc like '扑克三国杀%%'
        and b.classify <> 0
        and a.jiashuihj <> 0


        union all 

        SELECT  b.classify 产品大类, 
        b.classify_1 产品中类, 
        b.classify_2 产品小类, 
        a.wuliaomc 产品名称,
        a.riqi,
        a.xiaoshousl ,
        a.danjulxmc,
        a.kehumc 客户,
        yearweek(a.riqi,1) 年周,
        case when yearweek(riqi,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 7 day),1) then '本周' 
            when yearweek(riqi,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 14 day),1) then '上周'
            else '其他' end 周分类,
        case when month(riqi)=month(CURRENT_DATE()) and year(riqi)=year(CURRENT_DATE())  then '本月' 
            when month(riqi)=month(CURRENT_DATE()) and year(riqi)=year(CURRENT_DATE())-1 and day(riqi)<day(CURRENT_DATE()) then '去年同期/本月' 
            when (month(riqi)=month(CURRENT_DATE())-1 and year(riqi)=year(CURRENT_DATE()) and day(riqi)<day(CURRENT_DATE()) and month(CURRENT_DATE())<>1) or (month(riqi)=12 and year(riqi)=year(CURRENT_DATE())-1 and day(riqi)<day(CURRENT_DATE()) and month(CURRENT_DATE())=1) then '上月同期'
            when (month(riqi)=month(CURRENT_DATE())-1 and year(riqi)=year(CURRENT_DATE()) and month(CURRENT_DATE())<>1) or  (month(riqi)=12 and year(riqi)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())=1)  then '上月'
            when (month(riqi)=month(CURRENT_DATE())-2 and year(riqi)=year(CURRENT_DATE()) and month(CURRENT_DATE())>2) or  (month(riqi)=11 and year(riqi)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())=1)  or  (month(riqi)=12 and year(riqi)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())=2) then '上月环比'
            when (month(riqi)=month(CURRENT_DATE())-1 and year(riqi)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())<>1) or  (month(riqi)=12 and year(riqi)=year(CURRENT_DATE())-2 and month(CURRENT_DATE())=1)  then '去年同期/上月'
            else '其他' end 月分类,
        case when (quarter(riqi)=quarter(CURRENT_DATE())-1 and quarter(CURRENT_DATE())<>1 and year(riqi)=year(CURRENT_DATE())) or (quarter(riqi)=4 and quarter(CURRENT_DATE())=1 and year(riqi)=year(CURRENT_DATE())-1)  then '上季度' 
            when (quarter(riqi)=quarter(CURRENT_DATE())-1 and quarter(CURRENT_DATE()) >2 and year(riqi)=year(CURRENT_DATE())) or (quarter(riqi)=3 and quarter(CURRENT_DATE())=1 and year(riqi)=year(CURRENT_DATE())-1)  or (quarter(riqi)=4 and quarter(CURRENT_DATE())=2 and year(riqi)=year(CURRENT_DATE())-1) then '上季度环比' 
            when quarter(riqi)=quarter(CURRENT_DATE())-1  and year(riqi)=year(CURRENT_DATE())-1 then '上季度同比'
            else '其他' end 季分类
        from erp_jd_dwd.erp_jd_dwd_dim_saleorders  a 
        LEFT JOIN(
            SELECT classify,classify_1,classify_2,wuliaomc 
            from erp_jd_dwd.erp_jd_dwd_fact_classify 
        ) b on a.wuliaomc = b.wuliaomc 
        where b.classify_1 = 'IP系列'
        and b.classify  in ('Yokakids','三国杀')



        union all 

        SELECT case when b.classify= '推理桌游' then '推理桌游（不含自采）' else b.classify  end 产品大类,
        b.classify_1 产品中类, 
        b.classify_2 产品小类, 
        a.wuliaomc 产品名称,
        a.riqi,
        a.xiaoshousl ,
        a.danjulxmc,
        a.kehumc 客户,
        yearweek(a.riqi,1) 年周,
        case when yearweek(riqi,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 7 day),1) then '本周' 
            when yearweek(riqi,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 14 day),1) then '上周'
            else '其他' end 周分类,
        case when month(riqi)=month(CURRENT_DATE()) and year(riqi)=year(CURRENT_DATE())  then '本月' 
            when month(riqi)=month(CURRENT_DATE()) and year(riqi)=year(CURRENT_DATE())-1 and day(riqi)<day(CURRENT_DATE()) then '去年同期/本月' 
            when (month(riqi)=month(CURRENT_DATE())-1 and year(riqi)=year(CURRENT_DATE()) and day(riqi)<day(CURRENT_DATE()) and month(CURRENT_DATE())<>1) or (month(riqi)=12 and year(riqi)=year(CURRENT_DATE())-1 and day(riqi)<day(CURRENT_DATE()) and month(CURRENT_DATE())=1) then '上月同期'
            when (month(riqi)=month(CURRENT_DATE())-1 and year(riqi)=year(CURRENT_DATE()) and month(CURRENT_DATE())<>1) or  (month(riqi)=12 and year(riqi)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())=1)  then '上月'
            when (month(riqi)=month(CURRENT_DATE())-2 and year(riqi)=year(CURRENT_DATE()) and month(CURRENT_DATE())>2) or  (month(riqi)=11 and year(riqi)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())=1)  or  (month(riqi)=12 and year(riqi)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())=2) then '上月环比'
            when (month(riqi)=month(CURRENT_DATE())-1 and year(riqi)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())<>1) or  (month(riqi)=12 and year(riqi)=year(CURRENT_DATE())-2 and month(CURRENT_DATE())=1)  then '去年同期/上月'
            else '其他' end 月分类,
        case when (quarter(riqi)=quarter(CURRENT_DATE())-1 and quarter(CURRENT_DATE())<>1 and year(riqi)=year(CURRENT_DATE())) or (quarter(riqi)=4 and quarter(CURRENT_DATE())=1 and year(riqi)=year(CURRENT_DATE())-1)  then '上季度' 
            when (quarter(riqi)=quarter(CURRENT_DATE())-1 and quarter(CURRENT_DATE()) >2 and year(riqi)=year(CURRENT_DATE())) or (quarter(riqi)=3 and quarter(CURRENT_DATE())=1 and year(riqi)=year(CURRENT_DATE())-1)  or (quarter(riqi)=4 and quarter(CURRENT_DATE())=2 and year(riqi)=year(CURRENT_DATE())-1) then '上季度环比' 
            when quarter(riqi)=quarter(CURRENT_DATE())-1  and year(riqi)=year(CURRENT_DATE())-1 then '上季度同比'
            else '其他' end 季分类
        from erp_jd_dwd.erp_jd_dwd_dim_saleorders  a 
        LEFT JOIN(
            SELECT classify,classify_1,classify_2,wuliaomc 
            from erp_jd_dwd.erp_jd_dwd_fact_classify 
        ) b on a.wuliaomc = b.wuliaomc 
        where b.classify = '推理桌游'
        and b.classify_1 not in ('自研B端剧本杀','其他','剧本杀配件','电商B端剧本杀','电商C端剧本杀','电商剧本杀道具') 


    ) a
    where  a.月分类 in ('本月','上月') or a.周分类 = '本周'
    group by a.产品大类,a.客户
) c on a.产品大类 = c.产品大类 and a.客户 = c.客户

where a.`部门` in ('批发流通事业组','零售事业组')
group by a.产品大类,a.`部门`,a.业务区域
);