/*
create table default.jxq_city_rk_1211 
(rk varchar
,city varchar)
*/

with 
forcast1 as (
select *, row_number() over(partition by channeltype,rk,beginmonth,endmonth order by month) month_rk
from default.jxq_orderqty_forecast
where channeltype = '猫超生鲜'
),

forcast2 as (
select a.*,b.city
      ,case 
       when month_rk = 1  then date_format(date_add('month',0 ,date_parse(beginmonth,'%Y%m')),'%Y%m') 
       when month_rk = 2  then date_format(date_add('month',1 ,date_parse(beginmonth,'%Y%m')),'%Y%m')
       when month_rk = 3  then date_format(date_add('month',2 ,date_parse(beginmonth,'%Y%m')),'%Y%m')
       when month_rk = 4  then date_format(date_add('month',3 ,date_parse(beginmonth,'%Y%m')),'%Y%m')
       when month_rk = 5  then date_format(date_add('month',4 ,date_parse(beginmonth,'%Y%m')),'%Y%m')
       when month_rk = 6  then date_format(date_add('month',5 ,date_parse(beginmonth,'%Y%m')),'%Y%m')
       when month_rk = 7  then date_format(date_add('month',6 ,date_parse(beginmonth,'%Y%m')),'%Y%m')
       when month_rk = 8  then date_format(date_add('month',7 ,date_parse(beginmonth,'%Y%m')),'%Y%m')
       when month_rk = 9  then date_format(date_add('month',8 ,date_parse(beginmonth,'%Y%m')),'%Y%m')
       when month_rk = 10 then date_format(date_add('month',9 ,date_parse(beginmonth,'%Y%m')),'%Y%m')
       when month_rk = 11 then date_format(date_add('month',10,date_parse(beginmonth,'%Y%m')),'%Y%m')
       when month_rk = 12 then date_format(date_add('month',11,date_parse(beginmonth,'%Y%m')),'%Y%m')
       end month_new
from forcast1 a
left join default.jxq_city_rk_1211 b
on a.rk = b.rk
)

,orderqty as (
select case when city like '%北京%' then '北京市' else city end as city
      ,month
      ,sum(orderqty) orderqty
from default.jxq_axd_subcity_bychannel 
where channeltype = '猫超生鲜'
group by case when city like '%北京%' then '北京市' else city end 
        ,month
)

,actual as (
select a.*
      ,nvl(b.orderqty,0)  orderqty
from forcast2 a
left join orderqty b 
on cast(a.city as varchar) = cast(b.city as varchar) 
and cast(a.month_new as varchar)= b.month 
),

base as (
select channeltype,beginmonth,endmonth,month_new,rk,city,cast(final_pred as double) final_pred,orderqty,abs(cast(final_pred as double)-orderqty) abs
from actual
)
select * from base

--by城市by月FA
select channeltype,beginmonth,endmonth,rk,city,sum(final_pred) final_pred,sum(orderqty) orderqty,sum(abs) abs,1-sum(abs)/sum(orderqty) fa 
from base
group by channeltype,beginmonth,endmonth,rk,city