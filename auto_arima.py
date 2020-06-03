#-*- coding:utf-8 -*-
from statsmodels.tsa.arima_model import ARIMA

def auto_arima(ts,u,w,v):

# ts: 时间序列
# u : arima(p,d,q)中p的循环上限
# w : arima(p,d,q)中d的循环上限
# v : arima(p,d,q)中q的循环上限

    s = 999999999999999999999999999999999999999
    p = 0
    q = 0
    d = 0
    for i in range(0,u+1):
        for j in range(0,v+1):
            for k in range(0,w+1):
                
                try:
                    model = ARIMA(ts, order=(i,k,j)).fit()
            
    #                 if diff == 1:
    #                     series = copy.deepcopy(ts)
    #                     t = 0
    #                     for r in range(1,len(ts)):
    #                         t = r-1
    #                         series[r] = 0 
    #                         series[r] = series[t] + model.fittedvalues[t]
    #                 else:
    #                     series = model.fittedvalues

    #                 RSS = np.sqrt(sum((series - ts) ** 2) / len(ts))
            
                    AIC = model.aic #选用aic最小的(p,d,q)组合作为输出值

                except:
                    continue

    #             if RSS < s:
                if AIC < s:
                    p = i
                    q = j
                    d = k
    #                 s = RSS
                    s = AIC

    return p,d,q

