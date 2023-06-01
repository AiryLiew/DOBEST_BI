drop table if exists www_bi_ads.ds_jd_data_compare;
CREATE TABLE www_bi_ads.ds_jd_data_compare (
SELECT 
`产品大类`,
       `产品中类`,
       `产品小类`,
       `产品名称`,
      ifnull(round(SUM(case when `年` = YEAR(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)) then 销量 end),1),0) 本年销量,
      ifnull(round(SUM(case when `年` = YEAR(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)) then 销售额 end),1),2) 本年销售额,
      ifnull(round(SUM(case when `年` = YEAR(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)) then 销售额 end)/SUM(case when `年` = YEAR(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)) then 销量 end),1),2) 本年客单价,
      ifnull(round(SUM(case when `年` = YEAR(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)) then 毛利 end),1),2) 本年毛利,
      ifnull(round(SUM(case when `年` = YEAR(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY))-1 then 销量 end),1),0) 去年销量,
      ifnull(round(SUM(case when `年` = YEAR(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY))-1 then 销售额 end),1),2) 去年销售额,
      ifnull(round(SUM(case when `年` = YEAR(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY))-1 then 销售额 end)/SUM(case when `年` = YEAR(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY))-1 then 销量 end),1),2) 去年客单价,
      ifnull(round(SUM(case when `年` = YEAR(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY))-1 then 毛利 end),1),2) 去年毛利
FROM www_bi_ads.ds_jd_data
WHERE `年` >= YEAR(DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY))-1
and 月_日<month(current_date())*100+day(current_date())
and 销售额<>0
and 产品大类<>'集换式卡牌'
group by `产品大类`,
       `产品中类`,
       `产品小类`,
       `产品名称`
);