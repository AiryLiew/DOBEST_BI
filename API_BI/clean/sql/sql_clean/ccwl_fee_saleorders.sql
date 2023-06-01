drop table if exists www_bi_ads.ccwl_fee_saleorders;
CREATE TABLE www_bi_ads.ccwl_fee_saleorders( 
  SELECT a.收货省,
        a.收货区域,
        a.`年`,
        a.`月`,
        sum(a.`总销量`) 总销量,
        round(sum(a.`总销售额`),2) 总销售额,
        round(sum(a.`毛利`),2) 毛利,
        b.B2B发货费用,
        b.仓储物流费用,
        c.发货费用,
        round(b.仓储物流费用*c.发货费用/b.B2B发货费用,2) 预估仓储物流费用
  from(
    SELECT a.kehumc 客户,
          a.`year` 年,
          a.`month` 月,
          a.`xiaoshousl` 总销量,
          a.`jiashuihj` 总销售额,
          a.`profit` 毛利,
          b.收货省,
          b.业务区域,
          b.收货区域
          FROM erp_jd_dwd.erp_jd_dwd_dim_saleorders a
    
    left join(
      SELECT DISTINCT  a.kehumc ,
      a.name_prov1 收货省,
      a.shouhuofdz ,
      a.kehufzmc  业务区域,
      b.region_name 收货区域
      FROM erp_jd_dwd.erp_jd_dwd_fact_client_c a
      
      left join(
        SELECT province_name,region_name 
        FROM baidu_map.ios_region_province
      ) b on a.name_prov1 = b.province_name
    ) b on a.kehumc = b.kehumc
    and a.shouhuofdz = b.shouhuofdz
    
    where a.bumen = '渠道'
  ) a





  left join(
    SELECT `年`,
          `月`,
          round(sum(`B2B发货费用`),2) B2B发货费用,
          round(sum(`合计`),2)  仓储物流费用
    FROM localdata.ccwl_fee
    where 部门 in ('批发流通事业组','零售事业组','线下渠道部')
    group by `年`,
          `月`
  ) b on a.`年`=b.`年`
  and a.`月`=b.`月`



  left join(
    SELECT a.目的地,
          round(sum(a.`总费用`),2)  发货费用,
          a.`年`,
          a.`月`
    FROM (
      SELECT case when `目的地` in ('金华','宁波','杭州') then '浙江' 
                  when `目的地` = '南京' then '江苏' else 目的地 end 目的地,
            `总费用`,
            `年`,
            `月`
      FROM localdata.ccwl_fee_ky
      where 部门 in ('批发流通事业组','零售事业组','线下渠道部')
    ) a
    group by a.`目的地`,a.`年`,
          a.`月`
  ) c on a.`年`= c.`年`
  and a.`月`= c.`月`
  and a.`收货省` = c.`目的地`


  where c.发货费用 IS NOT NULL 
  group by a.收货区域,a.收货省,
        a.`年`,
        a.`月`

);