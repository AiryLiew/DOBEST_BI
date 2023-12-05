delete from erp_jd_ads.`key_product_sales_area_cpgl`
where 日期>=DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 63 DAY), '%Y-%m-01') ;

INSERT INTO erp_jd_ads.`key_product_sales_area_cpgl`(部门,业务区域, 客户, 省1, 市1, 省, 市, 产品大类, 产品中类, 产品小类, 产品名称, 年, 季度, 月, 周,  日期, 总销量 , 总销售额, 毛利,分级, 地理区域, 本年等级, 上年等级)
select a.* ,
b.level 分级,
b.region_name 地理区域,
b.本年等级,
b.上年等级
from(
    SELECT *  
    FROM erp_jd_ads.key_product_sales_area a  
    WHERE  a.日期 >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 63 DAY), '%Y-%m-01')
    and (
        (  
            a.`产品中类` NOT IN ('海外系列', '阵面对决', 'IP系列', '自研B端剧本杀', '其他', '剧本杀配件', '电商剧本杀道具', '收藏卡')   
            AND a.`产品大类` IN ('欢乐坊', '推理桌游', '三国杀', 'Yokakids', '周边')  
            AND a.`产品名称` NOT LIKE '贵人鸟资源卡包第一弹%'  
            AND a.`产品名称` NOT LIKE '扑克三国杀%'  
        )  
        OR  a.`产品小类` = '其他闪'  
        OR  
        (  
            a.`产品大类` IN ('三国杀', 'Yokakids')  
            AND a.`产品中类` = 'IP系列'  
        )  
        OR  
        (  
            a.`产品名称` LIKE '扑克三国杀%'  
            AND a.`总销售额` <> 0  
        )  
        OR  a.`产品名称` LIKE '三国小百科%'  
        ) 
) a


left join(
    SELECT distinct 
    a.kehumc,
    c.region_name,
    x.level,
    d.本年等级,
    e.上年等级
    FROM erp_jd_dwd.erp_jd_dwd_fact_client a

    left join(
        SELECT province_name,
        region_name
        FROM baidu_map.ios_region_province
    ) c on c.province_name = a.name_prov1

    left join(
        SELECT * FROM baidu_map.citylevel
    ) x on x.city = a.name_city1

    left join(
        select a.`客户名称`,a.等级 本年等级
        from(
            select 
            a.*,
            case when a.销售额>=15 then 'S'
            when a.销售额>=6 then 'A' 
            when a.销售额>=1 then 'B'
            else 'C' end 等级
            from(
                select year(a.riqi) 年份,
                ifnull(b.客户,a.kehumc)  `客户名称`,
                round(sum(a.jiashuihj_ac)/10000,2) 销售额,
                round(sum(a.xiaoshousl_ac)/10000,2) 销量
                from erp_jd_dws.erp_jd_dws_saleordersqd a

                left join 
                    localdata.customer_name_change 
                b on a.kehumc=b.客户原抬头
                where year(a.riqi)>=year(DATE_ADD(CURRENT_DATE,interval -1 day))-3
                and danjulxmc!='年返销售订单'
                group by 1,2
            )a 
        )a 
        where a.年份 = year(current_date())
    ) d on d.客户名称 = a.kehumc

    left join(
        select a.`客户名称`,a.等级 上年等级
        from(
            select 
            a.*,
            case when a.销售额>=15 then 'S'
            when a.销售额>=6 then 'A' 
            when a.销售额>=1 then 'B'
            else 'C' end 等级
            from(
                select year(a.riqi) 年份,
                ifnull(b.客户,a.kehumc)  `客户名称`,
                round(sum(a.jiashuihj_ac)/10000,2) 销售额,
                round(sum(a.xiaoshousl_ac)/10000,2) 销量
                from erp_jd_dws.erp_jd_dws_saleordersqd a

                left join 
                    localdata.customer_name_change 
                b on a.kehumc=b.客户原抬头
                where year(a.riqi)>=year(DATE_ADD(CURRENT_DATE,interval -1 day))-3
                and danjulxmc!='年返销售订单'
                group by 1,2
            )a 
        )a 
        where a.年份 = year(current_date())-1
    ) e on e.客户名称 = a.kehumc

) b on b.kehumc = a.`客户` 
;