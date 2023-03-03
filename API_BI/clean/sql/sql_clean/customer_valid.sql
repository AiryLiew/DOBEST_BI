DROP TABLE IF EXISTS localdata.customer_valid;
CREATE TABLE localdata.customer_valid(
select a.* ,
b.等级 最近一次下单_等级,
c.等级 上一次下单_等级,
case when a.`是否有效客户1`='有效' and a.`是否有效客户2` is null then '新客'
	when a.`是否有效客户1`='有效' and a.`是否有效客户2` = '流失' then '回流'
	when a.`是否有效客户1`='有效' and a.`是否有效客户2` = '有效' then '有效'
	when a.`是否有效客户1`='流失' and a.`是否有效客户2` is null  then '1次下单流失'
	else '流失' end 客户状态,
round(datediff(current_date(),  a.`最近一次下单日期`)/30,0) 流失时长
from(
SELECT a.`kehumc` `客户名称`,
  a.`kehufzmc` `客户分组`,
  a.`businessarea` 区域,
  a.`name_prov1` 省1,
  a.`name_city1` 市1,
  a.`name_prov` 省,
  a.`name_city` 市,
  b.`最近一次下单日期`,
  year(b.`最近一次下单日期`) 最近一次下单_年,
b.订单数量 最近一次订单数量,
b.订单金额 最近一次订单金额,
b.`是否有效客户1`,
c.`上一次下单日期`,
year(c.`上一次下单日期`) 上一次下单_年,
c.订单数量 上一次订单数量,
c.订单金额 上一次订单金额,
c.`是否有效客户2`
FROM erp_jd_dwd.erp_jd_dwd_fact_client a

left join(
select b.kehumc `客户名称`  ,
b.riqi `最近一次下单日期`,
b.订单数量,
b.订单金额,
case when b.riqi>=date_sub(current_date(),interval 270 day) then '有效' else '流失' end `是否有效客户1`
from(
select a.* ,
rank() over(partition by a.kehumc order by a.riqi desc) 排名
from(
SELECT 
kehumc ,
riqi,
sum(xiaoshousl) 订单数量,
sum(jiashuihj) 订单金额
FROM erp_jd_dwd.erp_jd_dwd_dim_saleorders
where jiashuihj>0
group by kehumc ,riqi
) a
) b
where b.排名=1
) b on b.`客户名称` = a.kehumc


left join(
select b.kehumc `客户名称`  ,
b.riqi `上一次下单日期`,
b.订单数量,
b.订单金额,
case when b.riqi>=date_sub(current_date(),interval 540 day) then '有效' else '流失' end `是否有效客户2`
from(
select a.* ,
rank() over(partition by a.kehumc order by a.riqi desc) 排名
from(
SELECT 
kehumc ,
riqi,
sum(xiaoshousl) 订单数量,
sum(jiashuihj) 订单金额
FROM erp_jd_dwd.erp_jd_dwd_dim_saleorders
where jiashuihj>0
group by kehumc ,riqi
) a
) b
where b.排名=2
) c on c.`客户名称` = a.kehumc

where b.`最近一次下单日期` is not null
) a


left join(
select 
    a.*,
		case when a.销售额>=15 then 'S'
		     when a.销售额>=6 then 'A' 
				 when a.销售额>=1 then 'B'
				 else 'C' end 等级
 from 
(select year(a.riqi) 年份,
  ifnull(b.客户,a.kehumc)  `客户名称`,
  round(sum(a.jiashuihj_ac)/10000,2) 销售额,
	round(sum(a.xiaoshousl_ac)/10000,2) 销量
from erp_jd_dws.erp_jd_dws_saleordersqd a
left join localdata.customer_name_change b on a.kehumc=b.客户原抬头
where year(a.riqi)>=year(DATE_ADD(CURRENT_DATE,interval -1 day))-3
  and danjulxmc!='年返销售订单'
group by 1,2
) a 
) b on b.客户名称 = a.客户名称 and a.最近一次下单_年 = b.年份


left join(
select 
    a.*,
		case when a.销售额>=15 then 'S'
		     when a.销售额>=6 then 'A' 
				 when a.销售额>=1 then 'B'
				 else 'C' end 等级
 from 
(select year(a.riqi) 年份,
  ifnull(b.客户,a.kehumc)  `客户名称`,
  round(sum(a.jiashuihj_ac)/10000,2) 销售额,
	round(sum(a.xiaoshousl_ac)/10000,2) 销量
from erp_jd_dws.erp_jd_dws_saleordersqd a
left join localdata.customer_name_change b on a.kehumc=b.客户原抬头
where year(a.riqi)>=year(DATE_ADD(CURRENT_DATE,interval -1 day))-3
  and danjulxmc!='年返销售订单'
group by 1,2
) a 
) c on c.客户名称 = a.客户名称 and a.上一次下单_年 = c.年份
);