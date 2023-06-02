drop table if exists erp_jd_ads.`key_product_sales_month_cpgl`;
CREATE TABLE erp_jd_ads.`key_product_sales_month_cpgl` (
    SELECT b.classify 产品大类,
    b.classify_1 产品中类,
    b.classify_2 产品小类,
    a.year 年,
    a.wuliaomc `物料名称`,
    sum(a.shifasl) 销量,
    c.`库存结余（产品管理部统计）` 库存结余,
    c.`工厂结余`,
    ifnull(c.`库存结余（产品管理部统计）`,0)+ifnull(c.`工厂结余`,0) `库存结余+工厂结余`
    FROM erp_jd_dwd.erp_jd_dwd_dim_saleshipping a

    left join (
        SELECT 
        wuliaomc,
        classify,
        classify_1,
        classify_2
        FROM erp_jd_dwd.erp_jd_dwd_fact_classify
    ) b on a.wuliaomc = b.wuliaomc

    left join (
        SELECT 
        `物料名称`,
        `库存结余（产品管理部统计）`,
        `工厂结余`
        FROM erp_jd_ads.key_product
    ) c on a.wuliaomc = c.`物料名称`

    where a.year >= year(date_sub(current_date(),interval 1 day))-1
    and a.month = month(date_sub(current_date(),interval 1 day))
    and a.kehumc not in ( '杭州迷思文化创意有限公司', '杭州泳淳网络技术有限公司',  '杭州游卡文化创意有限公司','上海卡丫卡文化传播有限公司','杭州游卡文化创意有限公司拱墅区分公司' )
    and b.classify_1 not in ('海外系列','阵面对决','IP系列','自研B端剧本杀','其他','剧本杀配件','电商剧本杀道具','收藏卡') 
    and b.classify in ('欢乐坊','推理桌游','三国杀','Yokakids','周边')
    and a.wuliaomc not like '贵人鸟资源卡包第一弹%'
    and a.wuliaomc not like '扑克三国杀%'
    GROUP by a.wuliaomc,a.year


    union all

    SELECT b.classify 产品大类,
    b.classify_1 产品中类,
    b.classify_2 产品小类,
    a.year 年,
    a.wuliaomc `物料名称`,
    sum(a.shifasl) 销量,
    c.`库存结余（产品管理部统计）` 库存结余,
    c.`工厂结余`,
    ifnull(c.`库存结余（产品管理部统计）`,0)+ifnull(c.`工厂结余`,0) `库存结余+工厂结余`
    FROM erp_jd_dwd.erp_jd_dwd_dim_saleshipping a

    left join (
        SELECT 
        wuliaomc,
        classify,
        classify_1,
        classify_2
        FROM erp_jd_dwd.erp_jd_dwd_fact_classify
    ) b on a.wuliaomc = b.wuliaomc

    left join (
        SELECT 
        `物料名称`,
        `库存结余（产品管理部统计）`,
        `工厂结余`
        FROM erp_jd_ads.key_product
    ) c on a.wuliaomc = c.`物料名称`

    where a.year >= year(date_sub(current_date(),interval 1 day))-1
    and a.month = month(date_sub(current_date(),interval 1 day))
    and a.kehumc not in ( '杭州迷思文化创意有限公司', '杭州泳淳网络技术有限公司',  '杭州游卡文化创意有限公司','上海卡丫卡文化传播有限公司','杭州游卡文化创意有限公司拱墅区分公司' )
    and b.classify in ('Yokakids','三国杀')  
    and b.classify_1 = 'IP系列'
    GROUP by a.wuliaomc,a.year


    union all

    SELECT b.classify 产品大类,
    b.classify_1 产品中类,
    b.classify_2 产品小类,
    a.year 年,
    a.wuliaomc `物料名称`,
    sum(a.shifasl) 销量,
    c.`库存结余（产品管理部统计）` 库存结余,
    c.`工厂结余`,
    ifnull(c.`库存结余（产品管理部统计）`,0)+ifnull(c.`工厂结余`,0) `库存结余+工厂结余`
    FROM erp_jd_dwd.erp_jd_dwd_dim_saleshipping a

    left join (
        SELECT 
        wuliaomc,
        classify,
        classify_1,
        classify_2
        FROM erp_jd_dwd.erp_jd_dwd_fact_classify
    ) b on a.wuliaomc = b.wuliaomc

    left join (
        SELECT 
        `物料名称`,
        `库存结余（产品管理部统计）`,
        `工厂结余`
        FROM erp_jd_ads.key_product
    ) c on a.wuliaomc = c.`物料名称`

    where a.year >= year(date_sub(current_date(),interval 1 day))-1
    and a.month = month(date_sub(current_date(),interval 1 day))
    and a.kehumc not in ( '杭州迷思文化创意有限公司', '杭州泳淳网络技术有限公司',  '杭州游卡文化创意有限公司','上海卡丫卡文化传播有限公司','杭州游卡文化创意有限公司拱墅区分公司' )
    and b.classify_2 = '其他闪'
    GROUP by a.wuliaomc,a.year



    union all

    SELECT b.classify 产品大类,
    b.classify_1 产品中类,
    b.classify_2 产品小类,
    a.year 年,
    a.wuliaomc `物料名称`,
    sum(a.shifasl) 销量,
    c.`库存结余（产品管理部统计）` 库存结余,
    c.`工厂结余`,
    ifnull(c.`库存结余（产品管理部统计）`,0)+ifnull(c.`工厂结余`,0) `库存结余+工厂结余`
    FROM erp_jd_dwd.erp_jd_dwd_dim_saleshipping a

    left join (
        SELECT 
        wuliaomc,
        classify,
        classify_1,
        classify_2
        FROM erp_jd_dwd.erp_jd_dwd_fact_classify
    ) b on a.wuliaomc = b.wuliaomc

    left join (
        SELECT 
        `物料名称`,
        `库存结余（产品管理部统计）`,
        `工厂结余`
        FROM erp_jd_ads.key_product
    ) c on a.wuliaomc = c.`物料名称`

    where a.year >= year(date_sub(current_date(),interval 1 day))-1
    and a.month = month(date_sub(current_date(),interval 1 day))
    and a.kehumc not in ( '杭州迷思文化创意有限公司', '杭州泳淳网络技术有限公司',  '杭州游卡文化创意有限公司','上海卡丫卡文化传播有限公司','杭州游卡文化创意有限公司拱墅区分公司' )
    and a.wuliaomc like '扑克三国杀%'
    and a.jiashuihj<>0
    GROUP by a.wuliaomc,a.year
);