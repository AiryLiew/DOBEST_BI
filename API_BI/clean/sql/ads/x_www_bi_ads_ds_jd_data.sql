delete from www_bi_ads.ds_jd_data
where 日期>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01');

INSERT INTO www_bi_ads.ds_jd_data(产品大类,产品中类, 产品小类,年,季度,月,日, 月_日,年周,星期,时段, 日期,店铺,平台, 单据编号, 是否赠品, 产品名称, 销量, 客单价, 销售额, 毛利, 成本单价, 总成本,上市日期)
SELECT 
case when b.classify = '0' then '其他' 
    when b.classify is null then '其他' 
    else b.classify end 产品大类,
b.classify_1 产品中类,
b.classify_2 产品小类,
year(a.`riqi`) 年,
quarter(a.`riqi`) 季度,
month(a.`riqi`) 月,
day(a.`riqi`) 日,
month(a.`riqi`)*100+day(a.`riqi`) 月_日,
week(a.`riqi`,1) 年周,
weekday(a.`riqi`)+1 星期,
case  when weekday(current_date())>3 and a.`riqi`<= DATE_SUB(current_date(), INTERVAL weekday(current_date())-3 DAY) and a.`riqi`>date_sub(DATE_SUB(current_date(), INTERVAL weekday(current_date())-3 DAY),INTERVAL 7 DAY) 
      then '本周' 
      when weekday(current_date())<4 and a.`riqi`<=date_sub(DATE_SUB(current_date(), INTERVAL weekday(current_date())-3 DAY),INTERVAL 7 DAY)  and a.`riqi`>date_sub(DATE_SUB(current_date(), INTERVAL weekday(current_date())-3 DAY),INTERVAL 14 DAY) 
      then '本周' 
      when weekday(current_date())>3 and a.`riqi`<=date_sub(DATE_SUB(current_date(), INTERVAL weekday(current_date())-3 DAY),INTERVAL 7 DAY)  and a.`riqi`>date_sub(DATE_SUB(current_date(), INTERVAL weekday(current_date())-3 DAY),INTERVAL 14 DAY) 
      then '上周'
      when weekday(current_date())<4 and a.`riqi`<=date_sub(DATE_SUB(current_date(), INTERVAL weekday(current_date())-3 DAY),INTERVAL 14 DAY)  and a.`riqi`>date_sub(DATE_SUB(current_date(), INTERVAL weekday(current_date())-3 DAY),INTERVAL 21 DAY) 
      then '上周' end 时段,
date_format(a.`riqi`,'%Y-%m-%d') 日期,
a.`kehumc` 店铺,
case when a.`kehumc` in (
    '拼多多-游点点玩具专营店',
    '拼多多-游卡玩具专营店',
    '拼多多-YOKAGAMES游卡专卖店',
    '拼多多- 三国杀官方旗舰店',
    '剧本杀拼多多店') then '拼多多' 
    when a.`kehumc`='周边商城' then '小程序' 
    when a.`kehumc` in (
    '北京京东世纪贸易有限公司',
    '京东三国杀POP旗舰店',
    '三国杀游卡专卖店（京东）',
    '京东-剧本杀旗舰店') then '京东' 
    when a.`kehumc` in (
    '游卡桌游文化店',
    '三国杀旗舰店',
    '剧本杀旗舰店') then '淘系'    
    when a.`kehumc` in
    ('头条小店-游点点',
    '抖店-剧本杀了谁') then '抖音' 
    ELSE '其他' end 平台, 
a.`danjubh` 单据编号,
a.`shifouzp` 是否赠品,
a.`wuliaomc` 产品名称,
a.`shifasl` 销量,
a.`hanshuidj` 客单价,
a.`jiashuihj` 销售额,
a.`profit` 毛利,
a.`cost` 成本单价,
a.`purchases` 总成本  ,
c.`riqi` 上市日期
FROM erp_jd_dws.erp_jd_dws_saleship_return a


left join (
  SELECT classify,classify_1,classify_2,wuliaomc
  FROM erp_jd_dwd.erp_jd_dwd_fact_classify
) b on a.wuliaomc = b.wuliaomc


left join (
  SELECT wlmc_all,riqi FROM erp_jd_dws.erp_jd_dws_launchtime 
) c on a.wuliaomc = c.wlmc_all

where a.bumen = '电商平台部'
and a.kehumc in (
'游卡桌游文化店',
'拼多多-游点点玩具专营店',
'拼多多-游卡玩具专营店',
'拼多多-YOKAGAMES游卡专卖店',
'拼多多- 三国杀官方旗舰店',
'头条小店-游点点',
'周边商城',
'北京京东世纪贸易有限公司',
'京东三国杀POP旗舰店',
'京东-三国杀旗舰店（自营）',
'三国杀游卡专卖店（京东）',
'三国杀旗舰店',
'三国杀快手小店',
'三国杀得物店',
'三国杀小店',
'三国杀B站旗舰店',
'得物-品牌直发店',
'快手三国杀游卡专卖店',
'快手小店-游点点',
'三国杀天猫超市',
'网易（杭州）网络有限公司',
'京东-剧本杀旗舰店',
'剧本杀XWJ',
'剧本杀得物店',
'剧本杀拼多多店',
'剧本杀旗舰店',
'抖店-剧本杀了谁')
and a.riqi>=DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 63 DAY), '%Y-%m-01');