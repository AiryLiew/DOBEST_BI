drop table if exists xwj.xwj_jd_inventory;
CREATE TABLE xwj.xwj_jd_inventory( 
select a.*,含税成本,
库存*含税成本 库存成本
from(
SELECT riqi,
       wuliaomc,
       wuliaobm,
       cangkumc,
       sum(receiving) 入库数量,
       sum(shipping) 出库数量,
       sum(inventory) 库存
FROM erp_jd_dws.erp_jd_dws_warehouse a
where `cangkumc` in ('下沙-新物集仓','下沙-核心桌游仓','义乌-新物集仓','义乌-核心桌游仓','松歌-新物集仓','松歌-核心桌游仓','义乌-新卡仓')
group by riqi,
       wuliaobm,
       cangkumc) a

left join(
SELECT 
        row_number() over(partition by `物料名称` order by 账簿 desc ,`年期` desc) 排序,
            `物料编码`,
        case when 会计年<2024 and 账簿 = '杭州游卡文化创意有限公司拱墅区分公司' then `期末成本单价`
            else round(`期末成本单价`*1.13,2) end 含税成本
        FROM erp_jd_dwd.erp_jd_dwd_dim_cost_period
        where  账簿 is not null and `期末成本单价`>0

) c on a.wuliaobm = c.物料编码
and 排序 = 1
);