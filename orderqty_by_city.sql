drop table if exists default.jxq_axd_bycity_bychannel;

create table default.jxq_axd_bycity_bychannel as 

with T0 AS (SELECT DISTINCT areapriorname,areaname
            FROM da.fct_indexbase_fororderdetail_byct
            WHERE SK_CreateDateId >= '@begindate'
              AND SK_CreateDateId <= '@enddate'
            )

,b2c AS (
SELECT cast(fo.SK_CreateDateId as varchar(6)) as month
      ,case when lower(fo.upchannelname) = '天猫超市' then '猫超生鲜'
            when lower(fo.upchannelname) = '官网' then '易果官网'
            when lower(fo.upchannelname) = '天猫旗舰店' then '易果旗舰店'
            else '其他' end as channeltype
      ,CASE WHEN fo.areapriorname IN ( '北京(郊区)'
                                      ,'西城区'
                                      ,'朝阳区'
                                      ,'大兴区'
                                      ,'海淀区'
                                      ,'东城区'
                                      ,'丰台区'
                                      ,'昌平区'
                                      ,'北京'
                                      ) THEN '北京市'
            WHEN fo.areapriorname IN (  '普陀区'
                                       ,'杨浦区'
                                       ,'闵行区'
                                       ,'浦东新区'
                                       ,'宝山区'
                                       ,'卢湾区'
                                       ,'虹口区'
                                       ,'上海(郊区)'
                                       ,'闸北区'
                                       ,'松江区'
                                       ,'黄浦区'
                                       ,'徐汇区'
                                       ,'长宁区'
                                       ,'静安区'
                                       ,'上海'
                                      )  THEN '上海市'
            WHEN fo.areapriorname LIKE '%成都%' OR fo.areaname LIKE '%成都%' THEN '成都市'
            WHEN fo.areapriorname = '天津' THEN '天津市'
            WHEN fo.areapriorname = '重庆' THEN '重庆市'
            WHEN fo.areaname LIKE '%杭州%' OR fo.areapriorname = '杭州市' THEN '杭州市'
            WHEN fo.areaname LIKE '%南京%' THEN '南京市'
            WHEN fo.areaname LIKE '%宁波%' THEN '宁波市'
            WHEN fo.areaname LIKE '%石家庄%' THEN '石家庄市'
            WHEN fo.areapriorname = '湖北' AND fo.areaname = '省直辖' THEN '武汉市'
            ELSE fo.areaname END AS city
      ,count(DISTINCT orderid) as orderqty
    FROM da.fct_indexbase_fororderdetail_byct fo
    JOIN T0 ON fo.areapriorname = T0.areapriorname AND fo.areaname = T0.areaname
    WHERE 1 = 1
      AND fo.SK_CreateDateId >= '@begindate'
      AND fo.SK_CreateDateId <= '@enddate'
      AND fo.upchannelname <> '果易达'
      AND fo.orderusername <> '批发市场'
      AND fo.channelname <> '猫1线下订单'
      AND fo.maincommodityname not like '%券%'
      AND fo.highcategoryname not in ('活蟹','水产礼券')
      AND fo.upchannelname in ('天猫超市','官网','天猫旗舰店')
    GROUP BY cast(fo.SK_CreateDateId as varchar(6))
            ,case when lower(fo.upchannelname) = '天猫超市' then '猫超生鲜'
                  when lower(fo.upchannelname) = '官网' then '易果官网'
                  when lower(fo.upchannelname) = '天猫旗舰店' then '易果旗舰店'
                  else '其他' end
            ,CASE WHEN fo.areapriorname IN ( '北京(郊区)'
                                            ,'西城区'
                                            ,'朝阳区'
                                            ,'大兴区'
                                            ,'海淀区'
                                            ,'东城区'
                                            ,'丰台区'
                                            ,'昌平区'
                                            ,'北京'
                                            ) THEN '北京市'
                  WHEN fo.areapriorname IN (  '普陀区'
                                             ,'杨浦区'
                                             ,'闵行区'
                                             ,'浦东新区'
                                             ,'宝山区'
                                             ,'卢湾区'
                                             ,'虹口区'
                                             ,'上海(郊区)'
                                             ,'闸北区'
                                             ,'松江区'
                                             ,'黄浦区'
                                             ,'徐汇区'
                                             ,'长宁区'
                                             ,'静安区'
                                             ,'上海'
                                            )  THEN '上海市'
                  WHEN fo.areapriorname LIKE '%成都%' OR fo.areaname LIKE '%成都%' THEN '成都市'
                  WHEN fo.areapriorname = '天津' THEN '天津市'
                  WHEN fo.areapriorname = '重庆' THEN '重庆市'
                  WHEN fo.areaname LIKE '%杭州%' OR fo.areapriorname = '杭州市' THEN '杭州市'
                  WHEN fo.areaname LIKE '%南京%' THEN '南京市'
                  WHEN fo.areaname LIKE '%宁波%' THEN '宁波市'
                  WHEN fo.areaname LIKE '%石家庄%' THEN '石家庄市'
                  WHEN fo.areapriorname = '湖北' AND fo.areaname = '省直辖' THEN '武汉市'
                  ELSE fo.areaname END
)

,o2o as (
select cast(sk_createdateid as varchar(6)) as month
      ,'O2O' as channeltype
      ,case when areaname = '上海' then '上海市'
            when areaname = '北京' then '北京市'
            when areaname = '天津' then '天津市'
            when areaname = '重庆' then '重庆市'
            else areaname 
        end as city
      ,count(distinct(orderid)) as orderqty
from da.fct_o2o_orderdetails
where sk_createdateid >= '@begindate'
  and sk_createdateid <= '@enddate'
  and group3 in ('天猫闪店','饿了么')
  and warehousename not in ('惠州义和仓','青岛流亭城配仓')
group by cast(sk_createdateid as varchar(6))
        ,case when areaname = '上海' then '上海市'
              when areaname = '北京' then '北京市'
              when areaname = '天津' then '天津市'
              when areaname = '重庆' then '重庆市'
              else areaname 
          end
)

,T1 AS (select * from b2c union select * from o2o)

,T2 AS (
SELECT T1.*
FROM T1
JOIN default.jxq_axd_citylist c
ON T1.city = c.city
)

,T3 AS (
select city,channeltype,row_number()over(partition by channeltype order by orderqty desc) as rk
FROM (SELECT city,channeltype,sum(orderqty) orderqty FROM T2 GROUP BY city,channeltype))

,ato as (
select T3.rk,T2.* 
from T2 
left join T3
on T2.city = T3.city and T2.channeltype = T3.channeltype
)

select channeltype,rk,city,month,SUM(orderqty) as orderqty
from ato
group by channeltype,rk,city,month
order by channeltype,rk,month
;
