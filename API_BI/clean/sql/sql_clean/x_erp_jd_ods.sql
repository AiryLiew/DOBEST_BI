delete FROM erp_jd_ods.erp_jd_ods_dim_saleshipping_kyk_cwzx
where fid>=(
    select a.fid from(
        SELECT min(fid) fid FROM erp_jd_ods.erp_jd_ods_dim_saleshipping_kyk_cwzx
        where year(riqi) = year(date_sub(current_date(),interval 32 day)) and month(riqi) = month(date_sub(current_date(),interval 32 day))
    ) a
);

delete FROM erp_jd_ods.erp_jd_ods_dim_saleshipping_wc_cwzx
where fid>=(
    select a.fid from(
        SELECT min(fid) fid FROM erp_jd_ods.erp_jd_ods_dim_saleshipping_wc_cwzx
        where year(riqi) = year(date_sub(current_date(),interval 32 day)) and month(riqi) = month(date_sub(current_date(),interval 32 day))
    ) a
);

delete FROM erp_jd_ods.erp_jd_ods_dim_saleshipping_ms_cwzx
where fid>=(
    select a.fid from(
        SELECT min(fid) fid FROM erp_jd_ods.erp_jd_ods_dim_saleshipping_ms_cwzx
        where year(riqi) = year(date_sub(current_date(),interval 32 day)) and month(riqi) = month(date_sub(current_date(),interval 32 day))
    ) a
);

delete FROM erp_jd_ods.erp_jd_ods_dim_saleshipping_yc_cwzx
where fid>=(
    select a.fid from(
        SELECT min(fid) fid FROM erp_jd_ods.erp_jd_ods_dim_saleshipping_yc_cwzx
        where year(riqi) = year(date_sub(current_date(),interval 32 day)) and month(riqi) = month(date_sub(current_date(),interval 32 day))
    ) a
);


delete FROM erp_jd_ods.erp_jd_ods_dim_salereturn_kyk_cwzx
where fid>=(
    select a.fid from(
        SELECT min(fid) fid FROM erp_jd_ods.erp_jd_ods_dim_salereturn_kyk_cwzx
        where year(riqi) = year(date_sub(current_date(),interval 32 day)) and month(riqi) = month(date_sub(current_date(),interval 32 day))
    ) a
);

delete FROM erp_jd_ods.erp_jd_ods_dim_salereturn_wc_cwzx
where fid>=(
    select a.fid from(
        SELECT min(fid) fid FROM erp_jd_ods.erp_jd_ods_dim_salereturn_wc_cwzx
        where year(riqi) = year(date_sub(current_date(),interval 32 day)) and month(riqi) = month(date_sub(current_date(),interval 32 day))
    ) a
);

delete FROM erp_jd_ods.erp_jd_ods_dim_salereturn_yc_cwzx
where fid>=(
    select a.fid from(
        SELECT min(fid) fid FROM erp_jd_ods.erp_jd_ods_dim_salereturn_yc_cwzx
        where year(riqi) = year(date_sub(current_date(),interval 32 day)) and month(riqi) = month(date_sub(current_date(),interval 32 day))
    ) a
);


delete FROM erp_jd_ods.erp_jd_ods_dim_purchasereceiving_kyk_cwzx
where fid>=(
    select a.fid from(
        SELECT min(fid) fid FROM erp_jd_ods.erp_jd_ods_dim_purchasereceiving_kyk_cwzx
        where year(riqi) = year(date_sub(current_date(),interval 32 day)) and month(riqi) = month(date_sub(current_date(),interval 32 day))
    ) a
);

delete FROM erp_jd_ods.erp_jd_ods_dim_purchasereceiving_ms_cwzx
where fid>=(
    select a.fid from(
        SELECT min(fid) fid FROM erp_jd_ods.erp_jd_ods_dim_purchasereceiving_ms_cwzx
        where year(riqi) = year(date_sub(current_date(),interval 32 day)) and month(riqi) = month(date_sub(current_date(),interval 32 day))
    ) a
);

delete FROM erp_jd_ods.erp_jd_ods_dim_purchasereceiving_wc_cwzx
where fid>=(
    select a.fid from(
        SELECT min(fid) fid FROM erp_jd_ods.erp_jd_ods_dim_purchasereceiving_wc_cwzx
        where year(riqi) = year(date_sub(current_date(),interval 32 day)) and month(riqi) = month(date_sub(current_date(),interval 32 day))
    ) a
);

delete FROM erp_jd_ods.erp_jd_ods_dim_purchasereceiving_yc_cwzx
where fid>=(
    select a.fid from(
        SELECT min(fid) fid FROM erp_jd_ods.erp_jd_ods_dim_purchasereceiving_yc_cwzx
        where year(riqi) = year(date_sub(current_date(),interval 32 day)) and month(riqi) = month(date_sub(current_date(),interval 32 day))
    ) a
);


delete FROM erp_jd_ods.erp_jd_ods_dim_purchasereturn_kyk_cwzx
where fid>=(
    select a.fid from(
        SELECT min(fid) fid FROM erp_jd_ods.erp_jd_ods_dim_purchasereturn_kyk_cwzx
        where year(tuiliaorq) = year(date_sub(current_date(),interval 32 day)) and month(tuiliaorq) = month(date_sub(current_date(),interval 32 day))
    ) a
);

delete FROM erp_jd_ods.erp_jd_ods_dim_purchasereturn_ms_cwzx
where fid>=(
    select a.fid from(
        SELECT min(fid) fid FROM erp_jd_ods.erp_jd_ods_dim_purchasereturn_ms_cwzx
        where year(tuiliaorq) = year(date_sub(current_date(),interval 32 day)) and month(tuiliaorq) = month(date_sub(current_date(),interval 32 day))
    ) a
);

delete FROM erp_jd_ods.erp_jd_ods_dim_purchasereturn_wc_cwzx
where fid>=(
    select a.fid from(
        SELECT min(fid) fid FROM erp_jd_ods.erp_jd_ods_dim_purchasereturn_wc_cwzx
        where year(tuiliaorq) = year(date_sub(current_date(),interval 32 day)) and month(tuiliaorq) = month(date_sub(current_date(),interval 32 day))
    ) a
);

delete FROM erp_jd_ods.erp_jd_ods_dim_purchasereturn_yc_cwzx
where fid>=(
    select a.fid from(
        SELECT min(fid) fid FROM erp_jd_ods.erp_jd_ods_dim_purchasereturn_yc_cwzx
        where year(tuiliaorq) = year(date_sub(current_date(),interval 32 day)) and month(tuiliaorq) = month(date_sub(current_date(),interval 32 day))
    ) a
);


delete FROM erp_jd_ods.erp_jd_ods_dim_othersreceiving_kyk_cwzx
where fid>=(
    select a.fid from(
        SELECT min(fid) fid FROM erp_jd_ods.erp_jd_ods_dim_othersreceiving_kyk_cwzx
        where year(riqi) = year(date_sub(current_date(),interval 32 day)) and month(riqi) = month(date_sub(current_date(),interval 32 day))
    ) a
);

delete FROM erp_jd_ods.erp_jd_ods_dim_othersreceiving_wc_cwzx
where fid>=(
    select a.fid from(
        SELECT min(fid) fid FROM erp_jd_ods.erp_jd_ods_dim_othersreceiving_wc_cwzx
        where year(riqi) = year(date_sub(current_date(),interval 32 day)) and month(riqi) = month(date_sub(current_date(),interval 32 day))
    ) a
);

delete FROM erp_jd_ods.erp_jd_ods_dim_othersreceiving_yc_cwzx
where fid>=(
    select a.fid from(
        SELECT min(fid) fid FROM erp_jd_ods.erp_jd_ods_dim_othersreceiving_yc_cwzx
        where year(riqi) = year(date_sub(current_date(),interval 32 day)) and month(riqi) = month(date_sub(current_date(),interval 32 day))
    ) a
);


delete FROM erp_jd_ods.erp_jd_ods_dim_distributedout_wc_cwzx
where fid>=(
    select a.fid from(
        SELECT min(fid) fid FROM erp_jd_ods.erp_jd_ods_dim_distributedout_wc_cwzx
        where year(riqi) = year(date_sub(current_date(),interval 32 day)) and month(riqi) = month(date_sub(current_date(),interval 32 day))
    ) a
);


delete FROM erp_jd_ods.erp_jd_ods_dim_distributedin_wc_cwzx
where fid>=(
select a.fid from(
SELECT min(fid) fid FROM erp_jd_ods.erp_jd_ods_dim_distributedin_wc_cwzx
where year(riqi) = year(date_sub(current_date(),interval 32 day)) and month(riqi) = month(date_sub(current_date(),interval 32 day))
) a
);


delete FROM erp_jd_ods.erp_jd_ods_dim_allocation_ms_cwzx
where fid>=(
select a.fid from(
SELECT min(fid) fid FROM erp_jd_ods.erp_jd_ods_dim_allocation_ms_cwzx
where year(riqi) = year(date_sub(current_date(),interval 32 day)) and month(riqi) = month(date_sub(current_date(),interval 32 day))
) a
);

delete FROM erp_jd_ods.erp_jd_ods_dim_allocation_wc_cwzx
where fid>=(
select a.fid from(
SELECT min(fid) fid FROM erp_jd_ods.erp_jd_ods_dim_allocation_wc_cwzx
where year(riqi) = year(date_sub(current_date(),interval 32 day)) and month(riqi) = month(date_sub(current_date(),interval 32 day))
) a
);

delete FROM erp_jd_ods.erp_jd_ods_dim_allocation_yc_cwzx
where fid>=(
select a.fid from(
SELECT min(fid) fid FROM erp_jd_ods.erp_jd_ods_dim_allocation_yc_cwzx
where year(riqi) = year(date_sub(current_date(),interval 32 day)) and month(riqi) = month(date_sub(current_date(),interval 32 day))
) a
);


delete FROM erp_jd_ods.erp_jd_ods_dim_assemble_kyk_cwzx
where fid>=(
select a.fid from(
SELECT min(fid) fid FROM erp_jd_ods.erp_jd_ods_dim_assemble_wc_cwzx
where year(rukurq) = year(date_sub(current_date(),interval 32 day)) and month(rukurq) = month(date_sub(current_date(),interval 32 day))
) a
);

delete FROM erp_jd_ods.erp_jd_ods_dim_assemble_yc_cwzx
where fid>=(
select a.fid from(
SELECT min(fid) fid FROM erp_jd_ods.erp_jd_ods_dim_assemble_yc_cwzx
where year(rukurq) = year(date_sub(current_date(),interval 32 day)) and month(rukurq) = month(date_sub(current_date(),interval 32 day))
) a
);



delete FROM erp_jd_ods.erp_jd_ods_dim_voucher_wc_cwzx
where fVoucherID>=(
select a.fVoucherID from(
SELECT min(fVoucherID) fVoucherID FROM erp_jd_ods.erp_jd_ods_dim_voucher_wc_cwzx
where year(fdate) = year(date_sub(current_date(),interval 32 day)) and month(fdate) = month(date_sub(current_date(),interval 32 day))
) a
);

delete FROM erp_jd_ods.erp_jd_ods_dim_voucher_yc_cwzx
where fVoucherID>=(
select a.fVoucherID from(
SELECT min(fVoucherID) fVoucherID FROM erp_jd_ods.erp_jd_ods_dim_voucher_yc_cwzx
where year(fdate) = year(date_sub(current_date(),interval 32 day)) and month(fdate) = month(date_sub(current_date(),interval 32 day))
) a
);

delete FROM erp_jd_ods.erp_jd_ods_dim_voucher_ms_cwzx
where fVoucherID>=(
select a.fVoucherID from(
SELECT min(fVoucherID) fVoucherID FROM erp_jd_ods.erp_jd_ods_dim_voucher_ms_cwzx
where year(fdate) = year(date_sub(current_date(),interval 32 day)) and month(fdate) = month(date_sub(current_date(),interval 32 day))
) a
);

delete FROM erp_jd_ods.erp_jd_ods_dim_voucher_kyk_cwzx
where fVoucherID>=(
select a.fVoucherID from(
SELECT min(fVoucherID) fVoucherID FROM erp_jd_ods.erp_jd_ods_dim_voucher_kyk_cwzx
where year(fdate) = year(date_sub(current_date(),interval 32 day)) and month(fdate) = month(date_sub(current_date(),interval 32 day))
) a
);