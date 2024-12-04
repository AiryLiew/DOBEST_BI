drop table if exists xwj.xwj_glht_user_register_ltv;
CREATE TABLE xwj.xwj_glht_user_register_ltv( 

with t as (
SELECT `用户Id`,
       `规格实付款`,
       ifnull(`规格实付款`,0)-ifnull(`含税成本`,0) 毛利,
        case when 类目 is null then '其他' else 类目 end 类目, 
       str_to_date(`创建时间`,'%Y-%m-%d') 日期
FROM xwj.xwj_glht_orders_xianhuo a


left join (

SELECT id,
       phylum `类目`
FROM xwj.xwj_glht_orders_xianhuo_classify

) b on a. `商品id` = b.id


left join(
SELECT 
       `物料名称`,
       `含税成本`,
      row_number() over(partition by `物料名称` order by `日期` desc) 排序
FROM www_bi_ads.xwj_data

) c on a.规格名称 = c.物料名称
and 排序 = 1
 
where  订单状态 not in ('维权结束','已取消','交易超时','已退款') 
AND 退款状态 = '未退款'
),


t1 as (
SELECT `用户Id`,
 日期,
 ifnull(round(SUM(收入),2),0) 收入,
ifnull(round(SUM(毛利),2),0) 毛利
FROM(
SELECT cast('新物赏' as char) 业务,
`用户Id`,
 日期,
ifnull(round(SUM(case when  赏池类型 <> '赏币兑换' then 规格实付 end),2),0) 收入,
ifnull(round(SUM( 毛利（含税）),2),0) 毛利
from (
	SELECT `用户Id`,
	str_to_date(`日期`,'%Y-%m-%d') 日期,
	       `订单标题`,
	       `规格实付`,
	       `赏池类型`,
	       `购买数量`*`成本单价（含税）` 成本（含税）,
	       ifnull(`规格实付`,0) - ifnull(`购买数量`*`成本单价（含税）`,0) 毛利（含税）
	FROM xwj.xwj_glht_xinwushang_cost_calculation
) a
group by `用户Id`,
 日期

union all

SELECT cast('现货商城' as char) 业务,
`用户Id`,
日期,
ifnull(round(SUM(规格实付款),2),0) 收入,
ifnull(round(SUM(毛利),2),0) 毛利
from t
group by `用户Id`,
日期


union all


SELECT cast('预售' as char) 业务,
`用户Id`,
日期,
ifnull(round(SUM(实付款),2),0) 收入,
ifnull(round(SUM(实付款),2),0)  毛利
from (
SELECT `用户Id`,
str_to_date(`创建时间`,'%Y-%m-%d') 日期,
       `项目名称`,
       实付款,
       case when  b.商家id is null then 实付款*0.015 else 实付款*0.2 end 毛利
FROM xwj.xwj_glht_orders_yushou a

left join xwj.xwj_store_ziying_list b on a.商家名称 = b.商家名称

where  订单状态 not in ('维权结束','已取消','交易超时','已退款') 
AND 档位退款状态 = '未退款'
) a 
group by `用户Id`,
日期

) a

group by `用户Id`,
日期

) 


select  t1.*,
注册时间,
datediff(日期,注册时间) 新用户转化天数,
注册渠道,
是否推广
from t1

left join (
SELECT DISTINCT 
id,
       str_to_date(created_at,'%Y-%m-%d') 注册时间,
       CASE when device_info in ('',' ') or device_info is null then '小程序' else 'APP' end 注册渠道,
       CASE when channel_id in ('',' ') or channel_id is null then '非推广' else '推广' end 是否推广
FROM xwj.xwj_glht_orders_user_register 
where created_at>=date_sub(current_date(),interval 1 year)
and phone is not null
and phone  not in ('',' ')

) b on b.id = t1.`用户Id`

where 注册时间 is not null



);