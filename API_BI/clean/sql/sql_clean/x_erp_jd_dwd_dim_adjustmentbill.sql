drop table if exists erp_jd_dwd.erp_jd_dwd_dim_adjustmentbill;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_dim_adjustmentbill( 
    select  
    b.fadjustmentamount 调整金额,
    a.fdate 日期 ,
    year(a.fdate) 会计年,
    month(a.fdate) 会计期间,
    case when a.fbusinesstype = 0 then '异常余额调整'
        when a.fbusinesstype = 1 then '入库调整'
        when a.fbusinesstype = 2 then '生产领料调整'
        when a.fbusinesstype = 3 then '销售出库调整'
        when a.fbusinesstype = 4 then '其他出库调整'
        when a.fbusinesstype = 5 then '出库核算调整' end 业务类型,
    case when a.fdocumentstatus = 'Z' then '暂存（默认）'
        when a.fdocumentstatus = 'A' then '创建'
        when a.fdocumentstatus = 'B' then '审核中'
        when a.fdocumentstatus = 'C' then '已审核'
        when a.fdocumentstatus = 'D' then '重新审核' end 单据状态,
    g.fname 仓库,
    d.wuliaobm 物料编码,
    d.wuliaomc 物料名称
    from erp_jd_ods.erp_jd_ods_dim_ths_adjustmentbill_cwzx a  

    left join erp_jd_ods.erp_jd_ods_dim_ths_adjustmentbillentry_cwzx b on a.fid=b.fid  
    left join erp_jd_ods.erp_jd_ods_dim_ths_inivstockdimension_cwzx c  on c.fentryid =b.fdimeentryid
    left join erp_jd_dwd.erp_jd_dwd_fact_classify d on c.fmaterialid = d.fid
    left join erp_jd_ods.erp_jd_ods_dim_tbd_stockl_cwzx g on c.fstockid=g.fstockid

    where a.fforbidstatus = 'A'
);