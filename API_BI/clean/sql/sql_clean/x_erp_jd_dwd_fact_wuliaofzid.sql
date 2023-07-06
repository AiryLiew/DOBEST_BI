drop table if exists erp_jd_dwd.erp_jd_dwd_fact_wuliaofzid;
CREATE TABLE erp_jd_dwd.erp_jd_dwd_fact_wuliaofzid (
      select 
      g.fnumber wuliaofzid_0,
      a.fnumber wuliaofzid_1,
      b.fnumber wuliaofzid_2,
      c.fnumber wuliaofzid_3,
      h.fname wuliaofzmc_0,
      d.fname wuliaofzmc_1,
      e.fname wuliaofzmc_2,
      f.fname wuliaofzmc_3
      FROM erp_jd_ods.erp_jd_ods_dim_materialgroup_wc_cwzx g

      left join(
            select
            fid,
            fparentid,
            fnumber
            FROM erp_jd_ods.erp_jd_ods_dim_materialgroup_wc_cwzx 
            where length(ffullparentid)-length(replace(ffullparentid,'.',''))=1                     
      ) a on a.fparentid = g.fid

      left join(
            select
            fid,
            fparentid,
            fnumber
            FROM erp_jd_ods.erp_jd_ods_dim_materialgroup_wc_cwzx 
            where length(ffullparentid)-length(replace(ffullparentid,'.',''))=2                      
      ) b on b.fparentid = a.fid

      left join(
            SELECT 
            fid,
            fparentid,
            fnumber
            FROM erp_jd_ods.erp_jd_ods_dim_materialgroup_wc_cwzx 
            where length(ffullparentid)-length(replace(ffullparentid,'.',''))=3                  
      ) c on c.fparentid = b.fid

      left join erp_jd_ods.erp_jd_ods_dim_materialgroupl_wc_cwzx d on a.fid = d.fid
      left join erp_jd_ods.erp_jd_ods_dim_materialgroupl_wc_cwzx e on b.fid = e.fid
      left join erp_jd_ods.erp_jd_ods_dim_materialgroupl_wc_cwzx f on c.fid = f.fid
      left join erp_jd_ods.erp_jd_ods_dim_materialgroupl_wc_cwzx h on g.fid = h.fid            

      where length(g.ffullparentid)-length(replace(g.ffullparentid,'.',''))=0  

);