drop table if exists erp_jd_ads.key_product_launchtime;
CREATE TABLE erp_jd_ads.key_product_launchtime(
    SELECT 
    b.`产品大类`  ,
    b.`产品中类`  ,
    b.`产品小类`  ,
    a.wlmc_all `产品名称` ,
    a.`上市时间` ,
    a.riqi `上市日期` ,
    c.`销量` ,
    f.`前3个月总销量` ,
    e.`采购数量` ,
    d.`采购入库量` 
    from(
        SELECT *,
        case when wlmc_all is not null then '上市当月' end 上市时间
        FROM erp_jd_dws.erp_jd_dws_launchtime

        union all

        SELECT wlmc_all,
        riqi,
        case when `month`=12 then `year`+1 else `year` end `year`,
        case when `month`=12 then 1 else `month`+1 end `month`,
        case when wlmc_all is not null then '上市第2月' end 上市时间
        FROM erp_jd_dws.erp_jd_dws_launchtime

        union all

        SELECT wlmc_all,
        riqi,
        case when `month` in (11,12) then `year`+1 else `year` end `year`,
        case when `month`=12 then 2 when `month`=11 then 1 else `month`+2 end `month`,
        case when wlmc_all is not null then '上市第3月' end 上市时间
        FROM erp_jd_dws.erp_jd_dws_launchtime
    ) a

    left join(
        select classify `产品大类`  ,
        classify_1 `产品中类`  ,
        classify_2 `产品小类`  ,
        wuliaomc `物料名称` 
        FROM erp_jd_dwd.erp_jd_dwd_fact_classify
    ) b on b.`物料名称` = a.wlmc_all

    left join(
        SELECT wuliaomc,
        year(riqi) 年,
        month(riqi) 月,
        sum(shifasl) `销量`
        FROM erp_jd_dwd.erp_jd_dwd_dim_saleshipping 
        where kehumc not in ('杭州泳淳网络技术有限公司','杭州游卡文化创意有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司')
        group by wuliaomc,year(riqi) ,month(riqi) 
    ) c on c.wuliaomc = a.wlmc_all and c.年 = a.`year` and c.月=a.`month`

    left join(
        SELECT wlmc_all,
        year(riqi) 年,
        month(riqi) 月,
        sum(shifasl_new) 采购入库量 
        FROM erp_jd_dwd.erp_jd_dwd_dim_purchasereceiving 
        group by wlmc_all,year(riqi) ,month(riqi) 
    ) d on d.wlmc_all = a.wlmc_all and d.年 = a.`year` and d.月=a.`month`

    left join(
        SELECT wlmc_all,
        year(riqi) 年,
        month(riqi) 月,
        sum(caigousl_new) 采购数量
        FROM erp_jd_dwd.erp_jd_dwd_dim_purchaseorders
        group by wlmc_all,year(riqi) ,month(riqi) 
    ) e on e.wlmc_all = a.wlmc_all and e.年 = a.`year` and e.月=a.`month`

    left join(
        SELECT a.`产品名称`,
        sum(a.`销量`) `前3个月总销量` 
        from(
            SELECT 
            a.wlmc_all `产品名称` ,
            c.`销量` 
            from(
                SELECT *,
                case when wlmc_all is not null then '上市当月' end 上市时间
                FROM erp_jd_dws.erp_jd_dws_launchtime

                union all

                SELECT wlmc_all,
                riqi,
                case when `month`=12 then `year`+1 else `year` end `year`,
                case when `month`=12 then 1 else `month`+1 end `month`,
                case when wlmc_all is not null then '上市第2月' end 上市时间
                FROM erp_jd_dws.erp_jd_dws_launchtime

                union all

                SELECT wlmc_all,
                riqi,
                case when `month` in (11,12) then `year`+1 else `year` end `year`,
                case when `month`=12 then 2 when `month`=11 then 1 else `month`+2 end `month`,
                case when wlmc_all is not null then '上市第3月' end 上市时间
                FROM erp_jd_dws.erp_jd_dws_launchtime
            ) a

            left join(
                SELECT wuliaomc,
                year(riqi) 年,
                month(riqi) 月,
                sum(shifasl) `销量`
                FROM erp_jd_dwd.erp_jd_dwd_dim_saleshipping 
                where kehumc not in ('杭州泳淳网络技术有限公司','杭州游卡文化创意有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司')
                group by wuliaomc,year(riqi) ,month(riqi) 
            ) c on c.wuliaomc = a.wlmc_all and c.年 = a.`year` and c.月=a.`month`
        ) a
        group by a.`产品名称`
    ) f on f.`产品名称` = a.wlmc_all
);
