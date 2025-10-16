# Flask 라이브러리가 설치되어 있지 않다면, 터미널에서 pip install Flask 를 실행해주세요.
# pykrx 라이브러리가 설치되어 있지 않다면, 터미널에서 pip install pykrx 를 실행해주세요.
from flask import Flask, request, render_template
from pykrx import stock
from datetime import datetime, timedelta
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
        self.data = {}
        self.working_days = []
        self.last_updated = 0
        self.refresh_interval = refresh_interval_seconds
        self.lock = threading.Lock()
        # 서버 시작 시 첫 데이터 로딩
        self._fetch_data()

    def _fetch_data(self):
        print("\n[CACHE] 새로운 주식 데이터를 KRX에서 가져옵니다...")
        self.data = {}
        
        today_str = stock.get_nearest_business_day_in_a_week()
        today_dt = datetime.strptime(today_str, '%Y%m%d')
        start_dt = today_dt - timedelta(days=7)
        start_str = start_dt.strftime('%Y%m%d')

        all_days_in_range = stock.get_previous_business_days(fromdate=start_str, todate=today_str)
        
        # 마지막 4일을 선택하고, 순서를 뒤집어 최신순으로 만듭니다. (오늘, 어제, 그제, ...)
        recent_4_days_ts = all_days_in_range[-4:][::-1]
        self.working_days = [ts.strftime('%Y%m%d') for ts in recent_4_days_ts]

        for day in self.working_days:
            print(f"  - {day} 데이터 가져오는 중...")
            turnover_df = stock.get_market_cap(day, market="ALL")
            turnover_df['종목명'] = turnover_df.index.map(lambda x: stock.get_market_ticker_name(x))
            turnover_df['일일회전율'] = turnover_df.apply(lambda x: (x['거래량'] / x['상장주식수']) * 100 if x['상장주식수'] > 0 else 0, axis=1)
            
            fundamental_df = stock.get_market_fundamental(day, market="ALL")
            
            combined_df = turnover_df.join(fundamental_df, how='inner')

            combined_df = combined_df[(combined_df['PER'] != 0) & (combined_df['PBR'] != 0)]
            
            display_cols = ['종목명', 'PER', 'PBR', '일일회전율', '시가', '고가', '저가']
            existing_cols = [col for col in display_cols if col in combined_df.columns]
            
            self.data[day] = combined_df[existing_cols]
        
        self.last_updated = time.time()
        print("[CACHE] 데이터 로딩 완료.")

    def get_data(self):
        with self.lock:
            current_time = time.time()
            if (current_time - self.last_updated) > self.refresh_interval:
                print("\n[CACHE] 캐시가 오래되어 데이터를 갱신합니다...")
                self._fetch_data()
            else:
                print("\n[CACHE] 유효한 캐시 데이터를 반환합니다.")
        return self.data, self.working_days

app = Flask(__name__)

@app.route('/')
def show_table():
    sort_by = request.args.get('sort', default='turnover_desc', type=str)
    data_dict, working_days = StockDataCache().get_data()

    daily_data = []
    for day in working_days:
        df = data_dict.get(day)
        day_formatted = f"{day[0:4]}-{day[4:6]}-{day[6:8]}"
        day_info = {'date': day_formatted}

        if df is not None and not df.empty:
            if sort_by == 'per_asc' and 'PER' in df.columns:
                df_sorted = df.sort_values(by='PER')
            elif sort_by == 'pbr_asc' and 'PBR' in df.columns:
                df_sorted = df.sort_values(by='PBR')
            else: # 기본 정렬 및 turnover_desc
                df_sorted = df.sort_values(by='일일회전율', ascending=False)

            day_info['data'] = df_sorted.head(20)
        else:
            day_info['data'] = None
        
        daily_data.append(day_info)

    return render_template('show_data.html', daily_data=daily_data)

if __name__ == "__main__":
    StockDataCache()
    app.run(host='0.0.0.0', port=5000, debug=True)
