drop table if exists erp_jd_ads.key_product_sales;
CREATE TABLE erp_jd_ads.key_product_sales ( 
    select a.classify `产品大类`  ,
    a.classify_1 `产品中类`  ,
    a.classify_2 `产品小类`  ,
    x.wuliaomc `产品名称` ,
    x.bumen `部门`,
    year(x.riqi) 年,
    quarter(x.riqi) 季度,
    month(x.riqi) 月,
    sum(x.shifasl) 销量 ,
    sum(x.jiashuihj) 销售额,
    sum(case when x.company = '杭州游卡文化创意有限公司' then x.shifasl end)  `文创销量`,
    sum(case when x.company = '杭州游卡文化创意有限公司' then x.jiashuihj end)  `文创销售额`,
    sum(case when x.company <> '杭州游卡文化创意有限公司' then x.shifasl end)  `其他公司销量`,
    sum(case when x.company <> '杭州游卡文化创意有限公司' then x.jiashuihj end)  `其他公司销售额`
    FROM erp_jd_dwd.erp_jd_dwd_dim_saleshipping x

    left join(
        SELECT classify,classify_1,classify_2,wuliaomc 
        FROM erp_jd_dwd.erp_jd_dwd_fact_classify 
    ) a on x.wuliaomc = a.wuliaomc

    where x.bumen in('渠道','电商平台部')
    and x.kehumc not in ('杭州泳淳网络技术有限公司','杭州游卡文化创意有限公司','杭州迷思文化创意有限公司','上海卡丫卡文化传播有限公司','杭州游卡文化创意有限公司拱墅区分公司')
    group by x.wuliaomc ,
    x.bumen,
    year(x.riqi),
    month(x.riqi)
);