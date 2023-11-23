drop table if exists erp_jd_dwd.erp_jd_dwd_dim_cost_period;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_cost_period( 
    SELECT distinct
    cast(0 as char) 期初金额,
    cast(0 as char) 期初数量,
    m.期末金额,
    m.本期收入金额,
    m.本期收入数量,
    m.本期发出金额,
    m.本期发出数量,
    m.期末数量,
    g.fname 仓库,
    d.wuliaobm 物料编码,
    d.wuliaomc 物料名称,
    c.会计期间,
    c.会计年,
    c.年期,
    ifnull(m.本期发出金额/m.本期发出数量,0) 本期发出成本单价,
    ifnull(m.期末金额/m.期末数量,0) 期末成本单价,
    cast(0 as char) 期初成本单价,
    case when m.fdimensionid = '4' then '杭州游卡文化创意有限公司'
        when m.fdimensionid = '1' then '杭州迷思文化创意有限公司' 
        when m.fdimensionid = '5' then '杭州泳淳网络技术有限公司' 
        when m.fdimensionid = '14' then '杭州游卡文化创意有限公司拱墅区分公司' 
        when m.fdimensionid = '9' then '上海卡丫卡文化传播有限公司' 
        else null end 账簿

    from (
        select wuliaobm ,wuliaomc , fid from erp_jd_ods.erp_jd_ods_fact_classify_wc_dobest
        union all 
        select wuliaobm ,wuliaomc , fid from erp_jd_ods.erp_jd_ods_fact_classify_wc_cwzx
        union all 
        select wuliaobm ,wuliaomc , fid from erp_jd_ods.erp_jd_ods_fact_classify_wc01_cwzx
        union all 
        select wuliaobm ,wuliaomc , fid from erp_jd_ods.erp_jd_ods_fact_classify_ms_dobest
        union all 
        select wuliaobm ,wuliaomc , fid from erp_jd_ods.erp_jd_ods_fact_classify_ms_cwzx
        union all 
        select wuliaobm ,wuliaomc , fid from erp_jd_ods.erp_jd_ods_fact_classify_yc_xmgs
        union all 
        select wuliaobm ,wuliaomc , fid from erp_jd_ods.erp_jd_ods_fact_classify_yc_cwzx
        union all 
        select wuliaobm ,wuliaomc , fid from erp_jd_ods.erp_jd_ods_fact_classify_kyk_cwzx) d 

    left join (
        select fmaterialid,fstockid,fentryid
        from
        erp_jd_ods.erp_jd_ods_dim_ths_inivstockdimension_cwzx 
    ) b on b.fmaterialid = d.fid


    left join (
        SELECT  
        fdimeentryid,
        `fendinitkey`,
        fdimensionid,
        fid,
        `famount` 期末金额,
        `fcurrentinamount` 本期收入金额,
        `fcurrentinqty` 本期收入数量,
        `fcurrentoutamount` 本期发出金额,
        `foutstockqty` 本期发出数量,
        `fqty` 期末数量
        FROM erp_jd_ods.erp_jd_ods_dim_ths_inivbalanceh_cwzx 
        where `fendinitkey` = 1
        and fdimensionid in ('1','4','5','9','14')
        and not (`famount` = 0 and 
        `fcurrentinamount` = 0 and 
        `fcurrentinqty` = 0 and 
        `fcurrentoutamount` = 0 and 
        `foutstockqty` = 0 and 
        `fqty` = 0)

        union all

        SELECT  
        fdimeentryid,
        `fendinitkey`,
        fdimensionid,
        fid,
        `famount` 期末金额,
        `fcurrentinamount` 本期收入金额,
        `fcurrentinqty` 本期收入数量,
        `fcurrentoutamount` 本期发出金额,
        `foutstockqty` 本期发出数量,
        `fqty` 期末数量
        FROM erp_jd_ods.erp_jd_ods_dim_ths_inivbalance_cwzx
        where `fendinitkey` = 1
        and fdimensionid in ('1','4','5','9','14')
        and not (`famount` = 0 and 
        `fcurrentinamount` = 0 and 
        `fcurrentinqty` = 0 and 
        `fcurrentoutamount` = 0 and 
        `foutstockqty` = 0 and 
        `fqty` = 0)
    ) m on m.fdimeentryid = b.fentryid 


    left join(
        SELECT 
        fname,
        fstockid
        from erp_jd_ods.erp_jd_ods_dim_tbd_stockl_cwzx
    ) 
    g on b.fstockid=g.fstockid


    left join(
        SELECT `fid` ,
        `fdate` 日期,
        `fdimensionid` ,
        `fperiod` 会计期间,
        `fyear` 会计年,
        `fyearperiod` 年期
        FROM erp_jd_ods.erp_jd_ods_dim_outacctg_cwzx 
    ) c on m.fid = c.fid 
    
    
    union all 


    SELECT distinct 
    m.期初金额,
    m.期初数量,
    cast(0 as char) 期末金额,
    cast(0 as char) 本期收入金额,
    cast(0 as char) 本期收入数量,
    cast(0 as char) 本期发出金额,
    cast(0 as char) 本期发出数量,
    cast(0 as char) 期末数量,
    g.fname 仓库,
    d.wuliaobm 物料编码,
    d.wuliaomc 物料名称,
    c.会计期间,
    c.会计年,
    c.年期,
    cast(0 as char) 本期发出成本单价,
    cast(0 as char) 期末成本单价,
    ifnull(m.期初金额/m.期初数量,0) 期初成本单价,
    case when m.fdimensionid = '4' then '杭州游卡文化创意有限公司'
        when m.fdimensionid = '1' then '杭州迷思文化创意有限公司' 
        when m.fdimensionid = '5' then '杭州泳淳网络技术有限公司' 
        when m.fdimensionid = '14' then '杭州游卡文化创意有限公司拱墅区分公司' 
        when m.fdimensionid = '9' then '上海卡丫卡文化传播有限公司' 
        else null end 账簿

    from (
        select wuliaobm ,wuliaomc , fid from erp_jd_ods.erp_jd_ods_fact_classify_wc_dobest
        union all 
        select wuliaobm ,wuliaomc , fid from erp_jd_ods.erp_jd_ods_fact_classify_wc_cwzx
        union all 
        select wuliaobm ,wuliaomc , fid from erp_jd_ods.erp_jd_ods_fact_classify_wc01_cwzx
        union all 
        select wuliaobm ,wuliaomc , fid from erp_jd_ods.erp_jd_ods_fact_classify_ms_dobest
        union all 
        select wuliaobm ,wuliaomc , fid from erp_jd_ods.erp_jd_ods_fact_classify_ms_cwzx
        union all 
        select wuliaobm ,wuliaomc , fid from erp_jd_ods.erp_jd_ods_fact_classify_yc_xmgs
        union all 
        select wuliaobm ,wuliaomc , fid from erp_jd_ods.erp_jd_ods_fact_classify_yc_cwzx
        union all 
        select wuliaobm ,wuliaomc , fid from erp_jd_ods.erp_jd_ods_fact_classify_kyk_cwzx) d 

    left join (
        select fmaterialid,fstockid,fentryid
        from
        erp_jd_ods.erp_jd_ods_dim_ths_inivstockdimension_cwzx 
    ) b on b.fmaterialid = d.fid


    left join (
        SELECT  
        fdimeentryid,
        `fendinitkey`,
        fdimensionid,
        fid,
        `famount` 期初金额,
        `fqty` 期初数量
        FROM erp_jd_ods.erp_jd_ods_dim_ths_inivbalanceh_cwzx 
        where `fendinitkey` = 0
        and fdimensionid in ('1','4','5','9','14')
        and not (`famount` = 0 and 
        `fcurrentinamount` = 0 and 
        `fcurrentinqty` = 0 and 
        `fcurrentoutamount` = 0 and 
        `foutstockqty` = 0 and 
        `fqty` = 0)

        union all

        SELECT  
        fdimeentryid,
        `fendinitkey`,
        fdimensionid,
        fid,
        `famount` 期初金额,
        `fqty` 期初数量
        FROM erp_jd_ods.erp_jd_ods_dim_ths_inivbalance_cwzx
        where `fendinitkey` = 0
        and fdimensionid in ('1','4','5','9','14')
        and not (`famount` = 0 and 
        `fcurrentinamount` = 0 and 
        `fcurrentinqty` = 0 and 
        `fcurrentoutamount` = 0 and 
        `foutstockqty` = 0 and 
        `fqty` = 0)
    ) m on m.fdimeentryid = b.fentryid 


    left join(
        SELECT 
        fname,
        fstockid
        from erp_jd_ods.erp_jd_ods_dim_tbd_stockl_cwzx
    ) 
    g on b.fstockid=g.fstockid


    left join(
        SELECT `fid` ,
        `fdimensionid` ,
        `fperiod` 会计期间,
        `fyear` 会计年,
        `fyearperiod` 年期
        FROM erp_jd_ods.erp_jd_ods_dim_outacctg_cwzx 
    ) c on m.fid = c.fid 

);