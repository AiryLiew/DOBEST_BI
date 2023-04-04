drop table if exists erp_jd_ads.`key_product_sales_area_cpgl_classify`;
CREATE TABLE erp_jd_ads.`key_product_sales_area_cpgl_classify` (
SELECT a.产品大类,a.`部门`,
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
round(sum(case when a.季分类='上季度' then a.`总销售额` end)/sum(case when a.季分类='上季度同比' then a.`总销售额` end)-1,3) 上季度销售额同比
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
where a.`部门` in ('批发流通事业组','零售事业组')
group by a.产品大类,a.`部门`
);