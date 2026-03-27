import pandas as pd
import mysql.connector
import json
import numpy as np

def fetch_and_parse_data():
    try:
        # 1. DB 연결 (비밀번호만 본인 것으로 수정하세요!)
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="your_password", 
            database="ros_db"
        )
        
        # 2. SQL로 데이터 통째로 가져오기
        query = "SELECT ranges, action FROM turtle_logs"
        df_raw = pd.read_sql(query, conn)
        conn.close()
        
        if df_raw.empty:
            print(" DB에 쌓인 데이터가 없습니다")
            return None

        print(f" DB 연결 성공 총 {len(df_raw)}개의 행(Row)을 가져왔습니다.")

        # 3. JSON 문자열 -> 리스트 변환
        # DB에서 가져온 ranges는 텍스트 형태라 json.loads로 진짜 리스트로 만듭니다.
        df_raw['ranges'] = df_raw['ranges'].apply(json.loads)

        # 4. 리스트를 360개 컬럼으로 확장
        # .tolist()로 리스트를 풀어서 새 데이터프레임을 생성합니다.
        lidar_df = pd.DataFrame(df_raw['ranges'].tolist(), 
                                columns=[f'dist_{i}' for i in range(360)])

        # 5. 기존 'action' 컬럼과 합치기 (360개 거리 + 1개 액션 = 361개)
        df_final = pd.concat([lidar_df, df_raw['action']], axis=1)

        # 6. 결과 확인
        print("\n--- 데이터프레임 변환 완료 ---")
        print(f"전체 모양(Shape): {df_final.shape}")
        print(df_final.head()) # 상단 5개 데이터 미리보기
        
        return df_final

    except Exception as e:
        print(f" 오류 발생: {e}")
        return None

if __name__ == "__main__":
    df = fetch_and_parse_data()
    
    if df is not None:
        # (csv 파일 저장
        df.to_csv('processed_turtle_data.csv', index=False, encoding='utf-8-sig')
        print("\n 'processed_turtle_data.csv' 파일로 저장되었습니다!")