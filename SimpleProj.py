import requests
import time
from pykrx import stock
import datetime as dt

# --- 몽키 패칭(Monkey Patching) 설정 for POST ---
# requests.post 함수의 원본을 저장합니다.
original_requests_post = requests.post

# 추적 기능을 넣은 새로운 함수를 정의합니다.
def patched_requests_post(*args, **kwargs):
    print(f"--- 네트워크 POST 요청 감지! URL: {args[0]} ---")
    if 'data' in kwargs:
        print(f"--- 데이터: {kwargs['data']} ---")
    # 저장해둔 원본 함수를 호출하여 실제 요청을 수행합니다.
    return original_requests_post(*args, **kwargs)

# requests.post 함수를 우리가 만든 새로운 함수로 교체합니다.
requests.post = patched_requests_post
# --- 몽키 패칭 설정 종료 ---


def calDailyStockTurnOverRatio(workingDay,marketCapdf):
    print("Cal Daily Stock Turnover Ratio")
    marketCapdf['일일회전율'] = marketCapdf.apply(lambda x: (x['거래량'] / x['상장주식수']) *100, axis=1)
    return marketCapdf

def buildBasicStockData():
    workingDay = stock.get_nearest_business_day_in_a_week()
    print("Working Day:", workingDay)
    
    print("\n[INFO] 시가총액 정보 가져오는 중...")
    marketCapDf = stock.get_market_cap(workingDay,market="ALL")
    
    marketCapDf = calDailyStockTurnOverRatio(workingDay,marketCapDf)
    
    print("\n[INFO] 주식 기본 정보(fundamental) 가져오는 중...")
    fundDf = stock.get_market_fundamental(workingDay,market="ALL")
    
    combinedDf = marketCapDf.join(fundDf, how='inner')
    
    print("\n[INFO] 종목 코드 -> 종목명 변환 시작...")
    start_time = time.time()
    combinedDf['종목명'] = combinedDf.index.map(lambda x: stock.get_market_ticker_name(x))
    end_time = time.time()
    print(f"[INFO] 종목명 변환 완료. 소요 시간: {end_time - start_time:.2f}초")

    # PER 또는 PBR이 0이 아닌 데이터만 필터링
    combinedDf = combinedDf[(combinedDf['PER'] != 0) & (combinedDf['PBR'] != 0)]
    
    combinedDf = combinedDf.sort_values(by='일일회전율', ascending=False)
    return combinedDf
    
if __name__ == "__main__":
    basicInfoDf = buildBasicStockData()
    print(basicInfoDf.head(10))