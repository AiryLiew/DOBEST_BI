drop table if exists erp_jd_dwd.erp_jd_dwd_dim_cost_period;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_cost_period( 
    SELECT 
    a.物料名称,
    a.物料编码,
    a.会计期间,
    a.会计年,
    a.年期,
    sum(a.期末金额)/sum(a.期末数量) 成本单价
    from(
        SELECT distinct 
        m.期末金额,
        m.本期收入金额,
        m.本期收入数量,
        m.本期发出金额,
        m.本期发出数量,
        m.期末数量,
        g.fname 仓库,
        d.wuliaobm 物料编码,
        d.wuliaomc 物料名称,
        c.日期,
        c.会计期间,
        c.会计年,
        c.年期,
        m.期末金额/m.期末数量 成本单价,
        case when c.fdimensionid = '4' then '杭州游卡文化创意有限公司'
            when c.fdimensionid = '1' then '杭州迷思文化创意有限公司' 
            when c.fdimensionid = '5' then '杭州泳淳网络技术有限公司' 
            when c.fdimensionid = '14' then '杭州游卡文化创意有限公司拱墅区分公司' 
            when c.fdimensionid = '9' then '上海卡丫卡文化传播有限公司' 
            else null end 账簿

        from erp_jd_dwd.erp_jd_dwd_fact_classify d 

        left join (
            select fmaterialid,fstockid,fentryid
            from
            erp_jd_ods.erp_jd_ods_dim_ths_inivstockdimension_cwzx 
        )
        b on b.fmaterialid = d.fid


        left join (
            SELECT  
            fdimeentryid,
            `fendinitkey`,
            fid,
            `famount` 期末金额,
            `fcurrentinamount` 本期收入金额,
            `fcurrentinqty` 本期收入数量,
            `fcurrentoutamount` 本期发出金额,
            `foutstockqty` 本期发出数量,
            `fqty` 期末数量
            FROM erp_jd_ods.erp_jd_ods_dim_ths_inivbalanceh_cwzx 
            where `fendinitkey` = 1

            union all

            SELECT  
            fdimeentryid,
            `fendinitkey`,
            fid,
            `famount` 期末金额,
            `fcurrentinamount` 本期收入金额,
            `fcurrentinqty` 本期收入数量,
            `fcurrentoutamount` 本期发出金额,
            `foutstockqty` 本期发出数量,
            `fqty` 期末数量
            FROM erp_jd_ods.erp_jd_ods_dim_ths_inivbalance_cwzx
            where `fendinitkey` = 1

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
        ) c on 
        m.fid = c.fid 

    ) a
    where a.成本单价 is not null
    group by a.物料名称,
    a.物料编码,
    a.会计年,
    a.会计期间

);