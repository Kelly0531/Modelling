
#-*- coding:utf-8 -*-
'''
周期性时间序列预测
'''
import os
import numpy as np
import pandas as pd
from pandas.core import datetools
import matplotlib.pylab as plt
import datetime
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.arima_model import ARIMA
import auto_arima as ar
import warnings
# 采用加法模型拆分：observed = trend + seasonal + residual

warnings.filterwarnings("ignore")

def decompose_arima(ts        # 时间序列
                   ,test_size # 测试集长度
                   ,pred_size # 预测长度
                   ,freq      # decompose函数拆分周期
                   ,p,d,q     # trend auto_arima循环上限
                   ,P,D,Q     # seasonal auto_arima循环上限
                   ):

    train_size = len(ts) - int(test_size)
    train = ts[:int(train_size)]
    test = ts[-int(test_size):]

    decomposition = seasonal_decompose(train, freq=freq, two_sided=False)
    # two_sided: trend和residual 左边空了一段，如果设为True，则会出现左右两边都空出来的情况，
    # False保证序列在最后的时间也有数据，方便预测。

    trend = decomposition.trend
    seasonal = decomposition.seasonal
    residual = decomposition.resid

    # print("decomposition:")
    # decomposition.plot()
    # plt.show()

    # d = residual.describe()
    # delta = d['75%'] - d['25%']
    # low_error, high_error = (d['25%'] - 1 * delta, d['75%'] + 1 * delta)

    trend.dropna(inplace=True) # 删除NA值，对原对象直接修改保存
    trend_index = ar.auto_arima(trend,p,d,q) 
    p = trend_index[0]
    d = trend_index[1]
    q = trend_index[2]
    trend_model = ARIMA(trend,order=(p,d,q)).fit()

    seasonal.dropna(inplace=True) # 删除NA值，对原对象直接修改保存
    seasonal_index = ar.auto_arima(seasonal,P,D,Q)
    P = seasonal_index[0]
    D = seasonal_index[1]
    Q = seasonal_index[2]
    seasonal_model =  ARIMA(seasonal,order=(P,D,Q)).fit()

    trend_pred = trend_model.forecast(pred_size)[0]
    trend_stdev = trend_model.forecast(pred_size)[1]

    seasonal_pred = seasonal_model.forecast(pred_size)[0]
    seasonal_stdev = seasonal_model.forecast(pred_size)[1]
    
    values = []
    # low_conf_values = []
    # high_conf_values = []
    for i in range(0,pred_size):
        trend_part = trend_pred[i]
        # season_part = train_season[train_season.index.strftime('%m') == t.strftime('%m')].mean() 
        seasonal_part = seasonal_pred[i]

        # 趋势+周期+误差界限
        predict = trend_part + seasonal_part
        # low_bound = trend_part + season_part + low_error
        # high_bound = trend_part + season_part + high_error
        values.append(predict)
        # low_conf_values.append(low_bound)
        # high_conf_values.append(high_bound)
        
    pred_time_index= pd.date_range(start=train.index[-1], periods=pred_size+1, freq='M')[1:]
    
    trend_pred = pd.Series(trend_pred, index=pred_time_index, name='trend_pred')
    trend_stdev = pd.Series(trend_stdev, index=pred_time_index, name='trend_stdev')
    seasonal_pred = pd.Series(seasonal_pred, index=pred_time_index, name='seasonal_pred')
    seasonal_stdev = pd.Series(seasonal_stdev, index=pred_time_index, name='seasonal_stdev')
    final_pred = pd.Series(values, index=pred_time_index, name='final_pred')
    # low_conf = pd.Series(low_conf_values, index=pred_time_index, name='low_conf')
    # high_conf = pd.Series(high_conf_values, index=pred_time_index, name='high_conf')
    
    return trend_pred,trend_stdev,seasonal_pred,seasonal_stdev,final_pred,p,d,q,P,D,Q
