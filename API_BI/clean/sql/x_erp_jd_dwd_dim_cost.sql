drop table if exists erp_jd_dwd.erp_jd_dwd_dim_cost;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_cost( 
    select
    wuliaomc,cost
    from(
    select
    wuliaomc,cost,
    row_number() over(partition by wuliaomc order by 排序)  排序
    from(
    select
    wuliaomc,cost,
    cast(1 as char) 排序
    from 
    (select 
        a.wuliaomc,
        sum(shuliang) 采购数量,
    round(sum(shuliang*成本价)/10000,2) 采购成本,
        round(sum(shuliang*成本价)/sum(shuliang)*1.13,3) cost
    from 
    (select distinct danjubh,wuliaomc,rukurq,shuliang 
        from erp_jd_dwd.erp_jd_dwd_dim_assemble a
        where shiwulx='组装'
    )a left join 

    (select danjubh,rukurq,max(chengbenj) 成本价
        from erp_jd_dwd.erp_jd_dwd_dim_assemble a
        where shiwulx='组装子件' 
        group by 1,2
    )b on a.danjubh=b.danjubh
    group by 1
    )a 
    
    where a.cost is not null
    
    
    union all

    SELECT wlmc_all wuliaomc,
    round(sum(jiashuihj)/sum(caigousl_new),2) cost,
    cast(2 as char) 排序
    FROM erp_jd_dwd.erp_jd_dwd_dim_purchaseorders 
    group by wlmc_all
    having sum(jiashuihj)/sum(caigousl_new)<>0

    union all

    SELECT a.wuliaomc,
    round(sum(a.zongchengb)/sum(case when a.zongchengb is not null or a.zongchengb not in ('',' ') then a.shifasl end)*1.13,2) cost ,
    cast(3 as char) 排序
    FROM erp_jd_dwd.erp_jd_dwd_dim_saleshipping a

    ) a
    ) a
    where 排序 = 1

);
