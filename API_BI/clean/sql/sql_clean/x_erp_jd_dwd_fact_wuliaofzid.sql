drop table if exists erp_jd_dwd.erp_jd_dwd_fact_wuliaofzid;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_fact_wuliaofzid (
      SELECT 
      a.fnumber wuliaofzid_0,
      b.fname wuliaofzmc_0,
      c.wuliaofzid_1,
      c.wuliaofzmc_1,
      d.wuliaofzid_2,
      d.wuliaofzmc_2,
      e.wuliaofzid_3,
      e.wuliaofzmc_3
      FROM erp_jd_ods.erp_jd_ods_dim_materialgroup_wc_cwzx a

      left join(
            SELECT fid,fname
            FROM erp_jd_ods.erp_jd_ods_dim_materialgroupl_wc_cwzx 
      ) b on a.fid = b.fid



      left join(
            SELECT 
            a.fnumber wuliaofzid_1,
            b.fname wuliaofzmc_1
            FROM erp_jd_ods.erp_jd_ods_dim_materialgroup_wc_cwzx a

            left join(
                  SELECT fid,fname
                  FROM erp_jd_ods.erp_jd_ods_dim_materialgroupl_wc_cwzx 
            ) b on a.fid = b.fid
            where length(a.fnumber)=4
      ) c on left(c.wuliaofzid_1,2) = a.fnumber

      left join(
            SELECT 
            a.fnumber wuliaofzid_2,
            b.fname wuliaofzmc_2
            FROM erp_jd_ods.erp_jd_ods_dim_materialgroup_wc_cwzx a

            left join(
                  SELECT fid,fname
                  FROM erp_jd_ods.erp_jd_ods_dim_materialgroupl_wc_cwzx 
            ) b on a.fid = b.fid
            where length(a.fnumber)=6
      ) d on left(d.wuliaofzid_2,4) = c.wuliaofzid_1

      left join(
            SELECT 
            a.fnumber wuliaofzid_3,
            b.fname wuliaofzmc_3
            FROM erp_jd_ods.erp_jd_ods_dim_materialgroup_wc_cwzx a

            left join(
                  SELECT fid,fname
                  FROM erp_jd_ods.erp_jd_ods_dim_materialgroupl_wc_cwzx 
            ) b on a.fid = b.fid
            where length(a.fnumber)=8
      ) e on left(e.wuliaofzid_3,6) = d.wuliaofzid_2

      where length(a.fnumber)=2

);