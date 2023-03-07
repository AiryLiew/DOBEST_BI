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

