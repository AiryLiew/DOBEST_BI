delete from erp_jd_ads.`key_product_sales_fc_cpgl`
where 日期>=DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 63 DAY), '%Y-%m-01');

INSERT INTO erp_jd_ads.`key_product_sales_fc_cpgl`(产品大类, 产品中类, 产品小类, 产品名称, 日期, 年周, 分类,
    渠道仓销量, 渠道仓销售额 , 电商仓销量 , 电商仓销售额, 泳淳电商仓销量, 泳淳电商仓销售额, 总销量 , 总销售额, 毛利,赠品数量)
    SELECT `产品大类`,
        `产品中类`,
        `产品小类`,
        `产品名称`,
        `日期`,
        yearweek(日期,1) 年周,
        case when yearweek(日期,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 7 day),1) then '本周' 
        when yearweek(日期,1)=yearweek(DATE_SUB(CURRENT_DATE(),INTERVAL 14 day),1) then '上周'
        else '其他' end 分类,
        ifnull(`渠道仓销量`,0) `渠道仓销量`,
        ifnull(`渠道仓销售额`,0) `渠道仓销售额`,
        ifnull(`电商仓销量`,0) `电商仓销量`,
        ifnull(`电商仓销售额`,0) `电商仓销售额`,
        ifnull(`泳淳电商仓销量`,0) `泳淳电商仓销量`,
        ifnull(`泳淳电商仓销售额`,0) `泳淳电商仓销售额`,
        ifnull(`总销量`,0) `总销量`,
        ifnull(`总销售额`,0) `总销售额`,
        ifnull(`毛利`,0) `毛利`,
        ifnull(`赠品数量`,0) `赠品数量`
    FROM erp_jd_ads.key_product_sales_fc
    WHERE  日期 >= DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 63 DAY), '%Y-%m-01')
    and (
        (  
            `产品中类` NOT IN ('海外系列', '阵面对决', 'IP系列', '自研B端剧本杀', '其他', '剧本杀配件', '电商剧本杀道具', '收藏卡')   
            AND `产品大类` IN ('欢乐坊', '推理桌游', '三国杀', 'Yokakids', '周边')  
            AND `产品名称` NOT LIKE '贵人鸟资源卡包第一弹%'  
            AND `产品名称` NOT LIKE '扑克三国杀%'  
        )  
        OR  `产品小类` = '其他闪'  
        OR  
        (  
            `产品大类` IN ('三国杀', 'Yokakids')  
            AND `产品中类` = 'IP系列'  
        )  
        OR  
        (  
            `产品名称` LIKE '扑克三国杀%'  
            AND `总销售额` <> 0  
        )  
        OR  `产品名称` LIKE '三国小百科%'  
        ) 

;