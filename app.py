# coding=utf-8
from flask import Flask, request, jsonify
import ibm_db_dbi as dbi
import pandas as pd

app = Flask(__name__)

@app.route('/', methods=['GET'])
def test():
    return {"sucess": "api 部署 成功"}
    

@app.route('/test')
def your_view_function():
    # 假设 response_data 是你要返回的字典数据
    response_data = {
        "月薪人员A": "相关数据"
    }
    return jsonify(response_data), 200, {'Content-Type': 'application/json; charset=utf-8'}

    
@app.route('/bus_info', methods=['GET'])
def get_bus_info():
    # 从请求中获取参数
    params = request.args
    bus_route = params.get('bus_route', default=7, type=int)
    bus_stop = params.get('bus_stop', default="haight street and filmore street").lower()
    DATABASE = params.get('DATABASE')
    HOSTNAME = params.get('HOSTNAME')
    PORT = params.get('PORT')
    UID = params.get('UID')
    PWD = params.get('PWD')

    # 构建数据库连接字符串
    db2_dsn = f"DATABASE={DATABASE};HOSTNAME={HOSTNAME};PORT={PORT};PROTOCOL=TCPIP;UID={UID};PWD={PWD};SECURITY=SSL"

    
    
    # 连接数据库
    db2_connection = dbi.connect(db2_dsn)
    query = 'SELECT * FROM "L662001RTSQ"."BUS_SCHEDULE"'
    bus_df = pd.read_sql_query(query, con=db2_connection)
    
    # 过滤数据
    query_df = bus_df[(bus_df.BUS_ROUTE == bus_route)]
    query_df = query_df[(query_df.BUS_STOP.str.lower() == bus_stop)]
    
    # 检查结果并构造响应
    if query_df.shape[0] <= 0:
        response = {"Error": "There are no records available with this data"}
    else:
        columns = query_df.columns.tolist()
        columns=["bus_ROUTE","BUS_STOP"]

        response_list = []
        for index, row in query_df.iterrows():
            row_dict = {columns[i]: row[i] for i in range(len(columns))}
            response_list.append(row_dict)
        response={
                "bus_message": "Here are the bus details:",
                "data": response_list
            }
        
    # 关闭数据库连接
    db2_connection.close()
    
    # 返回JSON响应
    return jsonify(response)


  
    
@app.route('/fetch_employeeid_attendance_date', methods=['GET'])
def fetch_employeeid_attendance_date():
    # 从请求中获取参数
    params = request.args
    DATABASE = params.get('DATABASE')
    HOSTNAME = params.get('HOSTNAME')
    PORT = params.get('PORT')
    UID = params.get('UID')
    PWD = params.get('PWD')
    
    employee_id = params.get('employee_id')
    attendance_date = params.get('attendance_date').lower()

    # 构建数据库连接字符串
    db2_dsn = f"DATABASE={DATABASE};HOSTNAME={HOSTNAME};PORT={PORT};PROTOCOL=TCPIP;UID={UID};PWD={PWD};SECURITY=SSL"

    
    
    # 连接数据库
    db2_connection = dbi.connect(db2_dsn)
    # query = f'SELECT * FROM "4D91DE2A"."DAILY" where EMPLOYEE_ID=\'{employee_id}\''

    query = f'SELECT * FROM "4D91DE2A"."DAILY" where EMPLOYEE_ID=\'{employee_id}\' and attendance_date=\'{attendance_date}\''

    query_df = pd.read_sql_query(query, con=db2_connection)
    
    # 过滤数据
#     query_df = df_1[(df_1.EMPLOYEE_ID == employee_id)]
#     query_df = query_df[(query_df.ATTENDANCE_DATE == attendance_date)]
    
    # 检查结果并构造响应
    if query_df.shape[0] <= 0:
        response = {"Error": "There are no records available with this data"}
    else:
        columns = query_df.columns.tolist()

        response_list = []
        for index, row in query_df.iterrows():
            row_dict = {columns[i]: row[i] for i in range(len(columns))}
            response_list.append(row_dict)
        response={
                "result_message": "查询结果正确",
                "data": response_list
            }
        
    # 关闭数据库连接
    db2_connection.close()

    
    # 返回JSON响应
    return jsonify(response), 200, {'Content-Type': 'application/json; charset=utf-8'} 



@app.route('/fetch_employeeid_attendance_month', methods=['GET'])
def fetch_employeeid_attendance_month():
    # 从请求中获取参数
    params = request.args
    DATABASE = params.get('DATABASE')
    HOSTNAME = params.get('HOSTNAME')
    PORT = params.get('PORT')
    UID = params.get('UID')
    PWD = params.get('PWD')
    
    employee_id = params.get('employee_id')
    attendance_month = params.get('attendance_month').lower()

    # 构建数据库连接字符串
    db2_dsn = f"DATABASE={DATABASE};HOSTNAME={HOSTNAME};PORT={PORT};PROTOCOL=TCPIP;UID={UID};PWD={PWD};SECURITY=SSL"

    
    
    # 连接数据库
    db2_connection = dbi.connect(db2_dsn)
    query = f"""
SELECT
    DEPARTMENT,
    ATTENDANCE_MONTH,
    EMPLOYEE_ID,
    POSITION,
    SUM(ON_TIME_LATE) AS ON_TIME_LATE,
    SUM(LEAVE_EARLY_MINUTES) AS LEAVE_EARLY_MINUTES,
    SUM(ABSENCE_REGULAR_SHIFT) AS ABSENCE_REGULAR_SHIFT,
    SUM(REGULAR_SHIFT_HOURS) AS REGULAR_SHIFT_HOURS,
    SUM(OVERTIME_CARD_SWIPING) AS OVERTIME_CARD_SWIPING,
    SUM(OVERTIME_REQUEST) AS OVERTIME_REQUEST,
    SUM(OVERTIME_ON_WEEKDAY) AS OVERTIME_ON_WEEKDAY,
    SUM(OVERTIME_ON_WEEKEND) AS OVERTIME_ON_WEEKEND,
    SUM(OVERTIME_ON_HOLIDAY) AS OVERTIME_ON_HOLIDAY,
    SUM(CONSECUTIVE_SHIFT_HOURS) AS CONSECUTIVE_SHIFT_HOURS,
    SUM(TOTAL_OVERTIME_HOURS) AS TOTAL_OVERTIME_HOURS,
    SUM(PAID_LEAVE) AS PAID_LEAVE,
    SUM(UNPAID_LEAVE) AS UNPAID_LEAVE,
    SUM(BUSINESS_TRIP_REGULAR_SHIFT) AS BUSINESS_TRIP_REGULAR_SHIFT,
    SUM(WAITING_MATERIAL_HOURS) AS WAITING_MATERIAL_HOURS,
    SUM(NIGHT_SHIFT_COUNT) AS NIGHT_SHIFT_COUNT,
    SUM(MEAL_ALLOWANCE) AS MEAL_ALLOWANCE,
    SUM(PAID_VACATION) AS PAID_VACATION
FROM "4D91DE2A"."DAILY"
WHERE EMPLOYEE_ID = \'{employee_id}\' AND ATTENDANCE_MONTH = \'{attendance_month}\'
GROUP BY DEPARTMENT,ATTENDANCE_MONTH, EMPLOYEE_ID, POSITION
"""
    

    query_df = pd.read_sql_query(query, con=db2_connection)
    print(query_df)
    
    # 过滤数据
#     query_df = df_1[(df_1.EMPLOYEE_ID == employee_id)]
#     query_df = query_df[(query_df.ATTENDANCE_DATE == attendance_date)]
    
    # 检查结果并构造响应
    if query_df.shape[0] <= 0:
        response = {"Error": "There are no records available with this data"}
    else:
        columns = query_df.columns.tolist()

        response_list = []
        for index, row in query_df.iterrows():
            row_dict = {columns[i]: row[i] for i in range(len(columns))}
            response_list.append(row_dict)
        response={
                "result_message": "查询结果正确",
                "data": response_list
            }
        
    # 关闭数据库连接
    db2_connection.close()

    
    # 返回JSON响应
    return jsonify(response), 200, {'Content-Type': 'application/json; charset=utf-8'} 



@app.route('/fetch_department_attendance_month', methods=['GET'])
def fetch_department_attendance_month():
    # 从请求中获取参数
    params = request.args
    DATABASE = params.get('DATABASE')
    HOSTNAME = params.get('HOSTNAME')
    PORT = params.get('PORT')
    UID = params.get('UID')
    PWD = params.get('PWD')
    
    department = params.get('department')
    attendance_month = params.get('attendance_month').lower()

    # 构建数据库连接字符串
    db2_dsn = f"DATABASE={DATABASE};HOSTNAME={HOSTNAME};PORT={PORT};PROTOCOL=TCPIP;UID={UID};PWD={PWD};SECURITY=SSL"

    
    
    # 连接数据库
    db2_connection = dbi.connect(db2_dsn)
    query = f"""
SELECT
    DEPARTMENT,
    ATTENDANCE_MONTH,
    EMPLOYEE_ID,
    POSITION,
    SUM(ON_TIME_LATE) AS ON_TIME_LATE,
    SUM(LEAVE_EARLY_MINUTES) AS LEAVE_EARLY_MINUTES,
    SUM(ABSENCE_REGULAR_SHIFT) AS ABSENCE_REGULAR_SHIFT,
    SUM(REGULAR_SHIFT_HOURS) AS REGULAR_SHIFT_HOURS,
    SUM(OVERTIME_CARD_SWIPING) AS OVERTIME_CARD_SWIPING,
    SUM(OVERTIME_REQUEST) AS OVERTIME_REQUEST,
    SUM(OVERTIME_ON_WEEKDAY) AS OVERTIME_ON_WEEKDAY,
    SUM(OVERTIME_ON_WEEKEND) AS OVERTIME_ON_WEEKEND,
    SUM(OVERTIME_ON_HOLIDAY) AS OVERTIME_ON_HOLIDAY,
    SUM(CONSECUTIVE_SHIFT_HOURS) AS CONSECUTIVE_SHIFT_HOURS,
    SUM(TOTAL_OVERTIME_HOURS) AS TOTAL_OVERTIME_HOURS,
    SUM(PAID_LEAVE) AS PAID_LEAVE,
    SUM(UNPAID_LEAVE) AS UNPAID_LEAVE,
    SUM(BUSINESS_TRIP_REGULAR_SHIFT) AS BUSINESS_TRIP_REGULAR_SHIFT,
    SUM(WAITING_MATERIAL_HOURS) AS WAITING_MATERIAL_HOURS,
    SUM(NIGHT_SHIFT_COUNT) AS NIGHT_SHIFT_COUNT,
    SUM(MEAL_ALLOWANCE) AS MEAL_ALLOWANCE,
    SUM(PAID_VACATION) AS PAID_VACATION
FROM "4D91DE2A"."DAILY"
WHERE DEPARTMENT = \'{department}\' AND ATTENDANCE_MONTH = \'{attendance_month}\'
GROUP BY DEPARTMENT,ATTENDANCE_MONTH, EMPLOYEE_ID, POSITION
"""
    
    print(query)
    query_df = pd.read_sql_query(query, con=db2_connection)
    print(query_df)
    
    # 过滤数据
#     query_df = df_1[(df_1.EMPLOYEE_ID == employee_id)]
#     query_df = query_df[(query_df.ATTENDANCE_DATE == attendance_date)]
    
    # 检查结果并构造响应
    if query_df.shape[0] <= 0:
        response = {"Error": "There are no records available with this data"}
    else:
        columns = query_df.columns.tolist()

        response_list = []
        for index, row in query_df.iterrows():
            row_dict = {columns[i]: row[i] for i in range(len(columns))}
            response_list.append(row_dict)
        response={
                "result_message": "查询结果正确",
                "data": response_list
            }
        
    # 关闭数据库连接
    db2_connection.close()

    
    # 返回JSON响应
    return jsonify(response), 200, {'Content-Type': 'application/json; charset=utf-8'} 




@app.route('/fetch_department_attendance_day', methods=['GET'])
def fetch_department_attendance_day():
    # 从请求中获取参数
    params = request.args
    DATABASE = params.get('DATABASE')
    HOSTNAME = params.get('HOSTNAME')
    PORT = params.get('PORT')
    UID = params.get('UID')
    PWD = params.get('PWD')
    
    department = params.get('department')
    attendance_date = params.get('attendance_date').lower()

    # 构建数据库连接字符串
    db2_dsn = f"DATABASE={DATABASE};HOSTNAME={HOSTNAME};PORT={PORT};PROTOCOL=TCPIP;UID={UID};PWD={PWD};SECURITY=SSL"

    
    
    # 连接数据库
    db2_connection = dbi.connect(db2_dsn)
    query = f"""
SELECT
    DEPARTMENT,
    ATTENDANCE_DATE,
    EMPLOYEE_ID,
    POSITION,
    SUM(ON_TIME_LATE) AS ON_TIME_LATE,
    SUM(LEAVE_EARLY_MINUTES) AS LEAVE_EARLY_MINUTES,
    SUM(ABSENCE_REGULAR_SHIFT) AS ABSENCE_REGULAR_SHIFT,
    SUM(REGULAR_SHIFT_HOURS) AS REGULAR_SHIFT_HOURS,
    SUM(OVERTIME_CARD_SWIPING) AS OVERTIME_CARD_SWIPING,
    SUM(OVERTIME_REQUEST) AS OVERTIME_REQUEST,
    SUM(OVERTIME_ON_WEEKDAY) AS OVERTIME_ON_WEEKDAY,
    SUM(OVERTIME_ON_WEEKEND) AS OVERTIME_ON_WEEKEND,
    SUM(OVERTIME_ON_HOLIDAY) AS OVERTIME_ON_HOLIDAY,
    SUM(CONSECUTIVE_SHIFT_HOURS) AS CONSECUTIVE_SHIFT_HOURS,
    SUM(TOTAL_OVERTIME_HOURS) AS TOTAL_OVERTIME_HOURS,
    SUM(PAID_LEAVE) AS PAID_LEAVE,
    SUM(UNPAID_LEAVE) AS UNPAID_LEAVE,
    SUM(BUSINESS_TRIP_REGULAR_SHIFT) AS BUSINESS_TRIP_REGULAR_SHIFT,
    SUM(WAITING_MATERIAL_HOURS) AS WAITING_MATERIAL_HOURS,
    SUM(NIGHT_SHIFT_COUNT) AS NIGHT_SHIFT_COUNT,
    SUM(MEAL_ALLOWANCE) AS MEAL_ALLOWANCE,
    SUM(PAID_VACATION) AS PAID_VACATION
FROM "4D91DE2A"."DAILY"
WHERE DEPARTMENT = \'{department}\' AND ATTENDANCE_DATE = \'{attendance_date}\'
GROUP BY DEPARTMENT,ATTENDANCE_DATE, EMPLOYEE_ID, POSITION
"""
    
    print(query)
    query_df = pd.read_sql_query(query, con=db2_connection)
    print(query_df)
    
    # 过滤数据
#     query_df = df_1[(df_1.EMPLOYEE_ID == employee_id)]
#     query_df = query_df[(query_df.ATTENDANCE_DATE == attendance_date)]
    
    # 检查结果并构造响应
    if query_df.shape[0] <= 0:
        response = {"Error": "There are no records available with this data"}
    else:
        columns = query_df.columns.tolist()

        response_list = []
        for index, row in query_df.iterrows():
            row_dict = {columns[i]: row[i] for i in range(len(columns))}
            response_list.append(row_dict)
        response={
                "result_message": "查询结果正确",
                "data": response_list
            }
        
    # 关闭数据库连接
    db2_connection.close()

    
    # 返回JSON响应
    return jsonify(response), 200, {'Content-Type': 'application/json; charset=utf-8'} 


if __name__ == '__main__':
    app.run(debug=True,port=8080)
