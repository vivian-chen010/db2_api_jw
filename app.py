from flask import Flask, request, jsonify
import ibm_db_dbi as dbi
import pandas as pd

app = Flask(__name__)

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
        bus_time = query_df.SCHEDULED_ARRIVAL_TIME.iloc[0]
        response = {
            "bus_message": f"Bus {bus_route} is scheduled to arrive at {bus_stop} at {bus_time}"
        }
    
    # 关闭数据库连接
    db2_connection.close()
    
    # 返回JSON响应
    return jsonify(response)

if __name__ == '__main__':
    app.run(debug=True)