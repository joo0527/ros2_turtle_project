import time
import json
import mysql.connector
from roslibpy import Ros, Topic, Message

# MySQL 연결 설정 
try:
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="your_password",  
        database="ros_db"
    )
    cursor = db.cursor()
    print(" MySQL 연결 성공")
except Exception as e:
    print(f" MySQL 연결 실패: {e}")
    db = None

#  ROS 연결 설정 (WSL2 IP 확인) 
WSL_IP = '' # 본인의 WSL IP 주소
client = Ros(host=WSL_IP, port=9090)
velocity_topic = Topic(client, '/turtle1/cmd_vel', 'geometry_msgs/Twist')

def lidar_callback(message):
    # LiDAR 거리 데이터 리스트 (전체 데이터)
    ranges = message['ranges']
    
    # 판단을 위한 주요 방향 거리 추출
    front_dist = ranges[0]
    left_dist = ranges[90]
    right_dist = ranges[270]

    move_cmd = {
        'linear': {'x': 0.0, 'y': 0.0, 'z': 0.0},
        'angular': {'x': 0.0, 'y': 0.0, 'z': 0.0}
    }

    #  주행 판단 로직 (후진 포함) 
    if front_dist > 1.0:
        action = "STRAIGHT"
        move_cmd['linear']['x'] = 2.0
    elif left_dist > 1.0:
        action = "LEFT_TURN"
        move_cmd['angular']['z'] = 1.5
    elif right_dist > 1.0:
        action = "RIGHT_TURN"
        move_cmd['angular']['z'] = -1.5
    elif front_dist <= 1.0 and left_dist <= 1.0 and right_dist <= 1.0:
        action = "BACKWARD"
        move_cmd['linear']['x'] = -1.5
    else:
        action = "STAY / UNKNOWN"

    #  데이터 발행 및 실시간 출력 
    velocity_topic.publish(Message(move_cmd))
    print(f"[{action}]  F:{front_dist:.2f}m | L:{left_dist:.2f}m | R:{right_dist:.2f}m")

    #  MySQL 데이터 저장 (JSON 형태) 
    if db:
        try:
            # 리스트를 JSON 문자열로 변환하여 저장
            ranges_json = json.dumps(ranges)
            
            # 테이블 컬럼: id(자동), when(자동), ranges, action
            # 'when'은 예약어이므로 SQL문 작성 시 주의 (DB 설정에 따라 생략 가능)
            sql = "INSERT INTO turtle_logs (ranges, action) VALUES (%s, %s)"
            val = (ranges_json, action)
            
            cursor.execute(sql, val)
            db.commit() # 변경사항 확정
        except Exception as e: 
            print(f" DB 저장 오류: {e}")

# 구독 설정 (Lidar 데이터 수신)
listener = Topic(client, '/scan', 'sensor_msgs/LaserScan')

try:
    client.run()
    print(" 거북이 AI 제어 + MySQL 기록 시스템 가동")
    listener.subscribe(lidar_callback)
    while client.is_connected:
        time.sleep(1)
except KeyboardInterrupt:
    if db:
        db.close()
        print("\n DB 연결 종료")
    client.terminate()