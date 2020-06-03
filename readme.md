1. 主运行程序为：
   - order_forecast_by_city.py 
2. 调用的包涉及：
   - auto_arima.py -- 输入时间序列循环计算得出最佳的（p,d,q)参数组合
   - decompose_arima.py -- 将时间序列按加法模型拆解，并调用auto_arima进行参数计算
   - execute_sql.py -- 执行单句sql
   - get_data.py -- 根据";"分号拆分sql文件，并分段按序执行  
3. 用到的sql脚本为：
   - orderqty_by_city.sql --清洗大表销售数据
   - FA.sql -- 计算FA