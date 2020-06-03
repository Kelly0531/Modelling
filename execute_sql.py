# encoding:utf8
import pandas as pd
from pandas import DataFrame
from pyhive import presto

def execute_sql(sql):

    host = '172.17.2.55'
    p_port = u'9090'
    p_username = 'jiangxiaoqian01'

    conn = presto.connect(host, port=p_port, username = p_username)

    cursor = conn.cursor()

    cursor.execute(sql)

    columns = [col[0] for col in cursor.description]

    ## result = [dict(zip(columns,row)) for row in cursor.fetchall()]

    row = cursor.fetchall()

    row = list(row)
    row = [list(i) for i in row]

    result = DataFrame(row,columns = columns)

    return result

