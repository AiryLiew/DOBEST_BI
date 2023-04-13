drop table if exists erp_jd_ads.key_product_sales_fc_cpgl_classify;
CREATE TABLE erp_jd_ads.key_product_sales_fc_cpgl_classify(
    SELECT a.`产品大类`,
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
        SELECT `产品大类`,
        `产品中类`,
        `产品小类`,
        `产品名称`,
        yearweek(日期,1) 年周,
        year(日期) 年,
        month(日期) 月,
        quarter(日期) 季度,
        month(日期)*100+day(日期) 月_日,
        case when yearweek(日期,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 7 day),1) then '本周' 
            when yearweek(日期,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 14 day),1) then '上周'
            else '其他' end 周分类,
        case when month(日期)=month(CURRENT_DATE()) and year(日期)=year(CURRENT_DATE())  then '本月' 
            when month(日期)=month(CURRENT_DATE()) and year(日期)=year(CURRENT_DATE())-1 and day(日期)<day(CURRENT_DATE()) then '去年同期/本月' 
            when (month(日期)=month(CURRENT_DATE())-1 and year(日期)=year(CURRENT_DATE()) and day(日期)<day(CURRENT_DATE()) and month(CURRENT_DATE())<>1) or (month(日期)=12 and year(日期)=year(CURRENT_DATE())-1 and day(日期)<day(CURRENT_DATE()) and month(CURRENT_DATE())=1) then '上月同期'
            when (month(日期)=month(CURRENT_DATE())-1 and year(日期)=year(CURRENT_DATE()) and month(CURRENT_DATE())<>1) or  (month(日期)=12 and year(日期)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())=1)  then '上月'
            when (month(日期)=month(CURRENT_DATE())-2 and year(日期)=year(CURRENT_DATE()) and month(CURRENT_DATE())>2) or  (month(日期)=11 and year(日期)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())=1)  or  (month(日期)=12 and year(日期)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())=2) then '上月环比'
            when (month(日期)=month(CURRENT_DATE())-1 and year(日期)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())<>1) or  (month(日期)=12 and year(日期)=year(CURRENT_DATE())-2 and month(CURRENT_DATE())=1)  then '去年同期/上月'
            else '其他' end 月分类,
        case when (quarter(日期)=quarter(CURRENT_DATE())-1 and quarter(CURRENT_DATE())<>1 and year(日期)=year(CURRENT_DATE())) or (quarter(日期)=4 and quarter(CURRENT_DATE())=1 and year(日期)=year(CURRENT_DATE())-1)  then '上季度' 
            when (quarter(日期)=quarter(CURRENT_DATE())-1 and quarter(CURRENT_DATE()) >2 and year(日期)=year(CURRENT_DATE())) or (quarter(日期)=3 and quarter(CURRENT_DATE())=1 and year(日期)=year(CURRENT_DATE())-1)  or (quarter(日期)=4 and quarter(CURRENT_DATE())=2 and year(日期)=year(CURRENT_DATE())-1) then '上季度环比' 
            when quarter(日期)=quarter(CURRENT_DATE())-1  and year(日期)=year(CURRENT_DATE())-1 then '上季度同比'
            else '其他' end 季分类,
        ifnull(`总销量`,0) `总销量`,
        ifnull(`总销售额`,0) `总销售额`,
        ifnull(`毛利`,0) `毛利`
        FROM erp_jd_ads.key_product_sales_fc_cpgl


        UNION all

        SELECT case when `产品大类`= '推理桌游' then '推理桌游（不含自采）' else 产品大类 end 产品大类,
            `产品中类`,
            `产品小类`,
            `产品名称`,
            yearweek(日期,1) 年周,
            year(日期) 年,
            month(日期) 月,
            quarter(日期) 季度,
            month(日期)*100+day(日期) 月_日,
            case when yearweek(日期,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 7 day),1) then '本周' 
                when yearweek(日期,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 14 day),1) then '上周'
                else '其他' end 周分类,
            case when month(日期)=month(CURRENT_DATE()) and year(日期)=year(CURRENT_DATE())  then '本月' 
                when month(日期)=month(CURRENT_DATE()) and year(日期)=year(CURRENT_DATE())-1 and day(日期)<day(CURRENT_DATE()) then '去年同期/本月' 
                when (month(日期)=month(CURRENT_DATE())-1 and year(日期)=year(CURRENT_DATE()) and day(日期)<day(CURRENT_DATE()) and month(CURRENT_DATE())<>1) or (month(日期)=12 and year(日期)=year(CURRENT_DATE())-1 and day(日期)<day(CURRENT_DATE()) and month(CURRENT_DATE())=1) then '上月同期'
                when (month(日期)=month(CURRENT_DATE())-1 and year(日期)=year(CURRENT_DATE()) and month(CURRENT_DATE())<>1) or  (month(日期)=12 and year(日期)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())=1)  then '上月'
                when (month(日期)=month(CURRENT_DATE())-2 and year(日期)=year(CURRENT_DATE()) and month(CURRENT_DATE())>2) or  (month(日期)=11 and year(日期)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())=1)  or  (month(日期)=12 and year(日期)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())=2) then '上月环比'
                when (month(日期)=month(CURRENT_DATE())-1 and year(日期)=year(CURRENT_DATE())-1 and month(CURRENT_DATE())<>1) or  (month(日期)=12 and year(日期)=year(CURRENT_DATE())-2 and month(CURRENT_DATE())=1)  then '去年同期/上月'
                else '其他' end 月分类,
            case when (quarter(日期)=quarter(CURRENT_DATE())-1 and quarter(CURRENT_DATE())<>1 and year(日期)=year(CURRENT_DATE())) or (quarter(日期)=4 and quarter(CURRENT_DATE())=1 and year(日期)=year(CURRENT_DATE())-1)  then '上季度' 
                when (quarter(日期)=quarter(CURRENT_DATE())-1 and quarter(CURRENT_DATE()) >2 and year(日期)=year(CURRENT_DATE())) or (quarter(日期)=3 and quarter(CURRENT_DATE())=1 and year(日期)=year(CURRENT_DATE())-1)  or (quarter(日期)=4 and quarter(CURRENT_DATE())=2 and year(日期)=year(CURRENT_DATE())-1) then '上季度环比' 
                when quarter(日期)=quarter(CURRENT_DATE())-1  and year(日期)=year(CURRENT_DATE())-1 then '上季度同比'
                else '其他' end 季分类,
            ifnull(`总销量`,0) `总销量`,
            ifnull(`总销售额`,0) `总销售额`,
            ifnull(`毛利`,0) `毛利`
        FROM erp_jd_ads.key_product_sales_fc_cpgl
        where `产品大类` = '推理桌游'
        and `产品中类` not in ('电商B端剧本杀','电商C端剧本杀') 

    ) a
    group by a.`产品大类`
);