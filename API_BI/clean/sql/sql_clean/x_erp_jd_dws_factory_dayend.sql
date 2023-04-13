INSERT INTO erp_jd_dws.erp_jd_dws_factory_dayend (
    select a.classify `产品大类`,
    a.classify_1 `产品中类`,
    a.classify_2 `产品小类`,
    b.wlmc_all `成品物料名称`,
    sum(b.caigousl_new) `成品采购数量`,
    sum(b.leijirksl_new) `成品累计入库`,
    sum(b.shengyurksl_new) `成品剩余入库`,
    date_sub(current_date(),interval 1 day) `状态时间`
    from erp_jd_dwd.erp_jd_dwd_dim_purchaseorders b

    left join (
        SELECT classify,classify_1,classify_2,wuliaomc 
        FROM erp_jd_dwd.erp_jd_dwd_fact_classify
    ) a on a.wuliaomc = b.wlmc_all

    group by b.wlmc_all
    having sum(b.caigousl_new)>0
);
