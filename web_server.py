# Flask 라이브러리가 설치되어 있지 않다면, 터미널에서 pip install Flask 를 실행해주세요.
# pykrx 라이브러리가 설치되어 있지 않다면, 터미널에서 pip install pykrx 를 실행해주세요.
from flask import Flask, request, render_template
from pykrx import stock
import datetime as dt
import time
import threading

# --- Singleton Decorator ---
def singleton(class_):
    instances = {}
    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]
    return getinstance

# --- Cache Manager Class ---
@singleton
class StockDataCache:
    def __init__(self, refresh_interval_seconds=86400):
        self.data = None
        self.last_updated = 0
        self.refresh_interval = refresh_interval_seconds
        self.lock = threading.Lock()
        # 서버 시작 시 첫 데이터 로딩
        self._fetch_data()

    def _fetch_data(self):
        print("\n[CACHE] 새로운 주식 데이터를 KRX에서 가져옵니다...")
        # 기존 get_stock_data() 함수의 로직을 이곳으로 옮겼습니다.
        workingDay = stock.get_nearest_business_day_in_a_week()
        
        turnover_df = stock.get_market_cap(workingDay, market="ALL")
        turnover_df['종목명'] = turnover_df.index.map(lambda x: stock.get_market_ticker_name(x))
        turnover_df['일일회전율'] = turnover_df.apply(lambda x: (x['거래량'] / x['상장주식수']) * 100 if x['상장주식수'] > 0 else 0, axis=1)
        
        fundamental_df = stock.get_market_fundamental(workingDay, market="ALL")
        
        combined_df = turnover_df.join(fundamental_df, how='inner')
        
        display_cols = ['종목명', 'PER', 'PBR', '일일회전율', '시가', '고가', '저가']
        existing_cols = [col for col in display_cols if col in combined_df.columns]
        
        self.data = combined_df[existing_cols]
        self.last_updated = time.time()
        print("[CACHE] 데이터 로딩 완료.")

    def get_data(self):
        # 여러 요청이 동시에 캐시를 수정하는 것을 방지 (Thread-safe)
        with self.lock:
            current_time = time.time()
            # 캐시가 오래되었는지 확인 (예: 10분이 지났는지)
            if (current_time - self.last_updated) > self.refresh_interval:
                print("\n[CACHE] 캐시가 오래되어 데이터를 갱신합니다...")
                self._fetch_data()
            else:
                print("\n[CACHE] 유효한 캐시 데이터를 반환합니다.")
        return self.data

app = Flask(__name__)

@app.route('/')
def show_table():
    # URL 파라미터에서 정렬 기준을 가져옵니다.
    sort_by = request.args.get('sort', default='turnover_desc', type=str)

    # 싱글톤 캐시 관리자로부터 데이터를 가져옵니다.
    df = StockDataCache().get_data()

    # 파라미터에 따라 데이터프레임을 정렬합니다.
    if sort_by == 'per_asc' and 'PER' in df.columns:
        df = df.sort_values(by='PER')
    elif sort_by == 'pbr_asc' and 'PBR' in df.columns:
        df = df.sort_values(by='PBR')
    elif sort_by == 'turnover_desc' and '일일회전율' in df.columns:
        df = df.sort_values(by='일일회전율', ascending=False)
    else:
        # 기본 정렬
        if '일일회전율' in df.columns:
            df = df.sort_values(by='일일회전율', ascending=False)

    # 데이터프레임을 HTML 테이블로 변환합니다.
    html_table = df.to_html(index=False, classes='table', border=0)

    # HTML 템플릿을 렌더링하여 테이블을 표시합니다.
    return render_template('show_data.html', table_html=html_table)

if __name__ == "__main__":
    # 서버가 시작될 때 StockDataCache 인스턴스를 처음으로 생성하여 캐시를 초기화합니다.
    StockDataCache()
    app.run(host='0.0.0.0', port=5000, debug=True)
