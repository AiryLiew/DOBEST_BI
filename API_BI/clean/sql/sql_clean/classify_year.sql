drop table if exists www_bi_ads.classify_year;
CREATE TABLE www_bi_ads.classify_year( 
    SELECT 
    `classify` ,
    `classify_1`,
    `classify_2`,
    `wuliaomc`,
    case when `wuliaomc` is not null then 2019 end `year`
    FROM erp_jd_dwd.erp_jd_dwd_fact_classify 

    union all 

    SELECT 
    `classify` ,
    `classify_1`,
    `classify_2`,
    `wuliaomc`,
    case when `wuliaomc` is not null then 2020 end `year`
    FROM erp_jd_dwd.erp_jd_dwd_fact_classify 

    union all 

    SELECT 
    `classify` ,
    `classify_1`,
    `classify_2`,
    `wuliaomc`,
    case when `wuliaomc` is not null then 2021 end `year`
    FROM erp_jd_dwd.erp_jd_dwd_fact_classify 

    union all 


    SELECT 
    `classify` ,
    `classify_1`,
    `classify_2`,
    `wuliaomc`,
    case when `wuliaomc` is not null then 2022 end `year`
    FROM erp_jd_dwd.erp_jd_dwd_fact_classify 

    union all 

    SELECT 
    `classify` ,
    `classify_1`,
    `classify_2`,
    `wuliaomc`,
    case when `wuliaomc` is not null then 2023 end `year`
    FROM erp_jd_dwd.erp_jd_dwd_fact_classify

    union all 

    SELECT 
    `classify` ,
    `classify_1`,
    `classify_2`,
    `wuliaomc`,
    case when `wuliaomc` is not null then 2024 end `year`
    FROM erp_jd_dwd.erp_jd_dwd_fact_classify
);