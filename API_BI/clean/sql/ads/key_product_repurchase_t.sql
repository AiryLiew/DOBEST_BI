drop table if exists erp_jd_ads.key_product_repurchase_t;
CREATE TABLE erp_jd_ads.key_product_repurchase_t( 
    SELECT
    a.classify `产品大类` ,
    a.classify_1 `产品中类` ,
    a.classify_2 `产品小类` ,
    x.wuliaomc `物料名称` ,
    b.riqi `上市日期`  ,
    sum(x.`总订单量`) `总订单量`,
    sum(x.`订单数`) `订单数`,
    sum(case when x.`订单数`>1 then x.`订单数` end) `复购订单数`,
    count(x.kehumc)  `客户数量` ,
    count(case when x.`订单数`>1 then x.kehumc end) `复购客户数量` ,
    round(sum(case when x.`订单数`>1 then x.`订单数` end)/sum(x.`订单数`),2)  `订单复购率` ,
    round(count(case when x.`订单数`>1 then x.kehumc end)/count(x.kehumc),2)  `用户复购率`
    from(
        SELECT
            wuliaomc ,
            kehumc ,
            count(distinct (danjubh)) `订单数`,
            sum(xiaoshousl) `总订单量`
            FROM erp_jd_dwd.erp_jd_dwd_dim_saleorders
            where danjulxmc in ('标准销售订单','寄售销售订单')
            group by wuliaomc,kehumc
        ) x

        left join(
            SELECT classify,classify_1,classify_2,wuliaomc 
            FROM erp_jd_dwd.erp_jd_dwd_fact_classify 
        ) a on x.wuliaomc = a.wuliaomc

        left join(
            SELECT wlmc_all,riqi 
            FROM erp_jd_dws.erp_jd_dws_launchtime
        ) b on b.wlmc_all = x.wuliaomc

    group by x.wuliaomc
);