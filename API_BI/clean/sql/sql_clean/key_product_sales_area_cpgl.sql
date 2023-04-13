drop table if exists erp_jd_ads.`key_product_sales_area_cpgl`;
CREATE TABLE erp_jd_ads.`key_product_sales_area_cpgl` (
    select a.* ,
    b.level 分级,
    b.region_name 地理区域,
    b.本年等级,
    b.上年等级
    from(
        SELECT *
        FROM erp_jd_ads.key_product_sales_area a
        where a.`产品中类` not in ('海外系列','阵面对决','IP系列','自研B端剧本杀','其他','剧本杀配件','电商剧本杀道具','收藏卡') 
        and a.`产品大类` in ('欢乐坊','推理桌游','三国杀','Yokakids','周边')
        and a.`产品名称` not like '贵人鸟资源卡包第一弹%'
        and a.`产品名称` not like '扑克三国杀%'

        union all

        SELECT *
        FROM erp_jd_ads.key_product_sales_area a
        where a.`产品大类` = '周边'
        and a.`产品小类` = '其他闪'

        union all

        SELECT *
        FROM erp_jd_ads.key_product_sales_area a
        where a.`产品大类` in ( '三国杀','Yokakids')
        and a.`产品中类` = 'IP系列'


        union all

        SELECT *
        FROM erp_jd_ads.key_product_sales_area a
        where a.`产品名称` like '扑克三国杀%'
        and a.`总销售额` <>0

        union all

        SELECT *
        FROM erp_jd_ads.key_product_sales_area a
        where a.`产品名称` like '三国小百科%'
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
);