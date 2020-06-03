# encoding:utf8

from pyhive import presto
import pandas as pd
from pandas import DataFrame

# warnings.filterwarnings("ignore")

def get_data(filename,begindate,enddate):

    host = '172.17.2.55'
    p_port = u'9090'
    p_username = 'jiangxiaoqian01'
    
    conn = presto.connect(host, port=p_port, username = p_username)
    
    cursor = conn.cursor()

    ## 根据；拆分sql文件，分段执行SQL
    with open(u'/home/jiangxiaoqian/Sales_Forecast_For_B2C_O2O_Express/1_Scripts/Python/'+filename, 'r+') as f:
        sql_list = f.read().split(';')[:-1]  # 对sql文件每一段的最后加上;
        sql_list = [x.replace('\n', ' ') if '\n' in x else x for x in sql_list]  # 将每段sql里的换行符改成空格

    ## 循环执行sql语句

    for sql_item  in sql_list:

        sql_item = sql_item.replace('@begindate',begindate)
        sql_item = sql_item.replace('@enddate',enddate)

        print (sql_item)
   
        cursor.execute(sql_item)

        columns = [col[0] for col in cursor.description]

        ## result = [dict(zip(columns,row)) for row in cursor.fetchall()]
    
        row = cursor.fetchall()

        row = list(row)
        row = [list(i) for i in row]

        result = DataFrame(row,columns = columns)
        
        ## print(result)

    conn.close()
    
    return result # 返回最后一段sql的结果


## if __name__ == '__main__':
    
    ## data = get_data('salesamount_by_warehouse.sql','20180901','20181031')

    ## print(data)

    
