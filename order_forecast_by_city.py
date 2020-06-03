# encoding:utf8
import time
import pandas as pd
import numpy as np
from get_data import get_data
from execute_sql import *
from decompose_arima import decompose_arima
from pyhive import presto
from pandas import DataFrame
import warnings
import traceback
from df2sql import *

warnings.filterwarnings("ignore") ##忽略warnings报错

if __name__ == "__main__":
    
    startime = time.time()
    
    ## 输入起止时间清洗大表销售数据
    df = get_data('orderqty_by_city.sql','20150101','20181130')
    
    endtime1 = time.time()
    
    time1 = endtime1 - startime
    
    print("源数据表已刷新!")
    print("运行时间：" + str(time1))

    tmall = execute_sql("select channeltype,rk,city,cast(date_format(date_parse(month,'%Y%m'),'%Y-%m') as varchar(7)) as month,orderqty from default.jxq_axd_bycity_bychannel where channeltype = '猫超生鲜' order by channeltype,rk,month") 
    
    guanwang = execute_sql("select channeltype,rk,city,cast(date_format(date_parse(month,'%Y%m'),'%Y-%m') as varchar(7)) as month,orderqty from default.jxq_axd_bycity_bychannel where channeltype = '易果官网' and month >= '201701' order by channeltype,rk,month")
 
    qijiandian = execute_sql("select channeltype,rk,city,cast(date_format(date_parse(month,'%Y%m'),'%Y-%m') as varchar(7)) as month,orderqty from default.jxq_axd_bycity_bychannel where channeltype = '易果旗舰店' order by channeltype,rk,month")

    o2o = execute_sql("select channeltype,rk,city,cast(date_format(date_parse(month,'%Y%m'),'%Y-%m') as varchar(7)) as month,orderqty from default.jxq_axd_bycity_bychannel where channeltype = 'O2O' order by channeltype,rk,month")

    endtime2 = time.time()

    time2 = endtime2 - endtime1
    
    # print(time2)
    # print(tmall)
    
    list1 = []
    list2 = qijiandian["rk"].values.tolist()

    for i in list2:
        if i not in list1:
            list1.append(i)
    list1.sort()
    print(list1)


    for w in list1:
        print("rk："+str(w))
        
        try:
            data = qijiandian[qijiandian.rk == w].loc[:,['month','orderqty']]
            # print(data)
            
            ts = pd.Series(data.orderqty.values, index=data.month)
            ts.index = pd.to_datetime(ts.index,format='%Y-%m')
            # print(ts)
            # print(ts.index)
            
            trend_pred,trend_stdev,seasonal_pred,seasonal_stdev,final_pred,p,d,q,P,D,Q = decompose_arima(ts,0,14,12,6,1,6,12,0,6)
            
            print("trend最佳参数： "+ str(p) + "," + str(d)+ "," + str(q))
            print("seasonal最佳参数： "+ str(P) + "," + str(D)+ "," + str(Q))
            # print("final_pred:")
            # print(final_pred)
            # print("trend_pred:")
            # print(trend_pred)
            # print("seasonal_pred:")
            # print(seasonal_pred)
            
            dff = pd.DataFrame({'month':final_pred.index.strftime("%Y-%m"),'rk':w,
                                'final_pred':final_pred,
                                'trend_pred':trend_pred,'trend_stdev':trend_stdev,
                                'seasonal_pred':seasonal_pred,'seasonal_stdev':seasonal_stdev,
                                'trend_arima_p':p,'trend_arima_d':d,'trend_arima_q':q,
                                'seasonal_arima_p':P,'seasonal_arima_d':D,'seasonal_arima_q':Q
                                })
            dff_columns = ['rk','month','final_pred','trend_pred','trend_stdev','seasonal_pred','seasonal_stdev','trend_arima_p','trend_arima_d','trend_arima_q','seasonal_arima_p','seasonal_arima_d','seasonal_arima_q']
            df_result = dff.ix[:,dff_columns] ##固定列名
            
            sql_tmall = "select '猫超生鲜' as channeltype,'@rk' as rk,'@month' as month,'@final_pred' as final_pred,'@trend_pred' as trend_pred,'@trend_stdev' as trend_stdev,'@seasonal_pred' as seasonal_pred,'@seasonal_stdev' as seasonal_stdev,'@trend_arima_p' as trend_arima_p,'@trend_arima_d' as trend_arima_d,'@trend_arima_q' as trend_arima_q,'@seasonal_arima_p' as seasonal_arima_p,'@seasonal_arima_d' as seasonal_arima_d,'@seasonal_arima_q' as seasonal_arima_q,date_format(now(),'%Y%m%d') as execute_date,'@beginmonth' as beginmonth,'@endmonth' as endmonth"
            sql_guanwang = "select '易果官网' as channeltype,'@rk' as rk,'@month' as month,'@final_pred' as final_pred,'@trend_pred' as trend_pred,'@trend_stdev' as trend_stdev,'@seasonal_pred' as seasonal_pred,'@seasonal_stdev' as seasonal_stdev,'@trend_arima_p' as trend_arima_p,'@trend_arima_d' as trend_arima_d,'@trend_arima_q' as trend_arima_q,'@seasonal_arima_p' as seasonal_arima_p,'@seasonal_arima_d' as seasonal_arima_d,'@seasonal_arima_q' as seasonal_arima_q,date_format(now(),'%Y%m%d') as execute_date,'@beginmonth' as beginmonth,'@endmonth' as endmonth"
            sql_qijiandian = "select '易果旗舰店' as channeltype,'@rk' as rk,'@month' as month,'@final_pred' as final_pred,'@trend_pred' as trend_pred,'@trend_stdev' as trend_stdev,'@seasonal_pred' as seasonal_pred,'@seasonal_stdev' as seasonal_stdev,'@trend_arima_p' as trend_arima_p,'@trend_arima_d' as trend_arima_d,'@trend_arima_q' as trend_arima_q,'@seasonal_arima_p' as seasonal_arima_p,'@seasonal_arima_d' as seasonal_arima_d,'@seasonal_arima_q' as seasonal_arima_q,date_format(now(),'%Y%m%d') as execute_date,'@beginmonth' as beginmonth,'@endmonth' as endmonth"
            sql_o2o = "select 'O2O' as channeltype,'@rk' as rk,'@month' as month,'@final_pred' as final_pred,'@trend_pred' as trend_pred,'@trend_stdev' as trend_stdev,'@seasonal_pred' as seasonal_pred,'@seasonal_stdev' as seasonal_stdev,'@trend_arima_p' as trend_arima_p,'@trend_arima_d' as trend_arima_d,'@trend_arima_q' as trend_arima_q,'@seasonal_arima_p' as seasonal_arima_p,'@seasonal_arima_d' as seasonal_arima_d,'@seasonal_arima_q' as seasonal_arima_q,date_format(now(),'%Y%m%d') as execute_date,'@beginmonth' as beginmonth,'@endmonth' as endmonth"
            
            sql = sql_qijiandian
            
            print("df_result:")
            print(df_result)
            
            ## 把预测结果写进hive表中
            sql_e = "insert into default.jxq_orderqty_forecast"
            for i in range(0,len(df_result)):
                 
                result = df_result.iloc[[i]]
                print("result: ")
                print(result)
    
                rk               = str(result.rk.tolist()[0])
                month            = str(result.month.tolist()[0])
                final_pred       = str(result.final_pred.tolist()[0])
                trend_pred       = str(result.trend_pred.tolist()[0])
                trend_stdev      = str(result.trend_stdev.tolist()[0])
                seasonal_pred    = str(result.seasonal_pred.tolist()[0])
                seasonal_stdev   = str(result.seasonal_stdev.tolist()[0])
                trend_arima_p    = str(result.trend_arima_p.tolist()[0])
                trend_arima_d    = str(result.trend_arima_d.tolist()[0])
                trend_arima_q    = str(result.trend_arima_q.tolist()[0])
                seasonal_arima_p = str(result.seasonal_arima_p.tolist()[0])
                seasonal_arima_d = str(result.seasonal_arima_d.tolist()[0])
                seasonal_arima_q = str(result.seasonal_arima_q.tolist()[0])
                beginmonth = '201812'
                endmonth = '202001'

                sql_r = sql.replace('@rk',rk)
                sql_r = sql_r.replace('@month',month)
                sql_r = sql_r.replace('@final_pred',final_pred)
                sql_r = sql_r.replace('@trend_pred',trend_pred)
                sql_r = sql_r.replace('@trend_stdev',trend_stdev)
                sql_r = sql_r.replace('@seasonal_pred',seasonal_pred)
                sql_r = sql_r.replace('@seasonal_stdev',seasonal_stdev)
                sql_r = sql_r.replace('@trend_arima_p',trend_arima_p)
                sql_r = sql_r.replace('@trend_arima_d',trend_arima_d)
                sql_r = sql_r.replace('@trend_arima_q',trend_arima_q)
                sql_r = sql_r.replace('@seasonal_arima_p',seasonal_arima_p)
                sql_r = sql_r.replace('@seasonal_arima_d',seasonal_arima_d)
                sql_r = sql_r.replace('@seasonal_arima_q',seasonal_arima_q)
                sql_r = sql_r.replace('@beginmonth',beginmonth)
                sql_r = sql_r.replace('@endmonth',endmonth)

                ## print("sql_r: "+ sql_r)
                
                if i == 0:
                    sql_e = sql_e +" " + sql_r
                else:
                    sql_e = sql_e + " union all " + sql_r
                
                sql_r = sql

            print("sql_e: " + sql_e)
            sql_insert_result = execute_sql(sql_e)
            print("执行结果 ："+str(sql_insert_result))
    
            ## df_result.to_csv(r'./test.txt', header = None, index = None, sep = ',', mode = 'a') 
        except:
            traceback.print_exc()
            continue

        if w == '1':
            break 
        print(time.time())

