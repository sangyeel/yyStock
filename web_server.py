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
        self.data = {"KOSPI": {}, "KOSDAQ": {}}
        
        today_str = stock.get_nearest_business_day_in_a_week()
        today_dt = datetime.strptime(today_str, '%Y%m%d')
        start_dt = today_dt - timedelta(days=7)
        start_str = start_dt.strftime('%Y%m%d')

        all_days_in_range = stock.get_previous_business_days(fromdate=start_str, todate=today_str)
        
        recent_4_days_ts = all_days_in_range[-4:][::-1]
        self.working_days = [ts.strftime('%Y%m%d') for ts in recent_4_days_ts]

        for market in ["KOSPI", "KOSDAQ"]:
            print(f"\n>> {market} 시장 데이터 처리 중...")
            for day in self.working_days:
                print(f"  - {day} 데이터 가져오는 중...")
                turnover_df = stock.get_market_cap(day, market=market)
                turnover_df['종목명'] = turnover_df.index.map(lambda x: stock.get_market_ticker_name(x))
                turnover_df['일일회전율'] = turnover_df.apply(lambda x: (x['거래량'] / x['상장주식수']) * 100 if x['상장주식수'] > 0 else 0, axis=1)
                
                fundamental_df = stock.get_market_fundamental(day, market=market)
                ohlcv_df = stock.get_market_ohlcv(day, market=market)
                
                combined_df = turnover_df.join(fundamental_df, how='inner')
                if not ohlcv_df.empty and '등락률' in ohlcv_df.columns:
                    combined_df = combined_df.join(ohlcv_df['등락률'], how='inner')
                
                display_cols = ['종목명', '거래량', '일일회전율', '등락률', '종가', '고가', '저가']
                existing_cols = [col for col in display_cols if col in combined_df.columns]
                
                self.data[market][day] = combined_df[existing_cols]
        
        self.last_updated = time.time()
        print("\n[CACHE] 데이터 로딩 완료.")

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

@app.context_processor
def utility_processor():
    def get_color_for_rate(rate):
        if not isinstance(rate, (int, float)):
            return '#FFFFFF'  # white

        # Clamp the rate for color calculation
        clamped_rate = max(-30.0, min(30.0, rate))

        if clamped_rate >= 0:
            # Interpolate from white to light red
            green_blue = int(255 - (180 * clamped_rate / 30))  # Ends at 75
            return f'rgb(255,{green_blue},{green_blue})'
        else:  # rate < 0
            # Interpolate from white to light blue
            clamped_rate = abs(clamped_rate)
            red_green = int(255 - (180 * clamped_rate / 30))  # Ends at 75
            return f'rgb({red_green},{red_green},255)'
    return dict(get_color_for_rate=get_color_for_rate)


@app.route('/')
def show_table():
    market = request.args.get('market', default='KOSPI', type=str)
    all_data, working_days = StockDataCache().get_data()
    data_dict = all_data.get(market, {})

    daily_data = []
    for day in working_days:
        df = data_dict.get(day)
        day_formatted = f"{day[0:4]}-{day[4:6]}-{day[6:8]}"
        day_info = {'date': day_formatted}

        if df is not None and not df.empty:
            df_sorted = df.sort_values(by='거래량', ascending=False)
            columns_to_show = ['종목명', '거래량', '일일회전율', '등락률', '종가']

            # Ensure we only select columns that actually exist in the DataFrame
            existing_columns_to_show = [col for col in columns_to_show if col in df.columns]
            df_display = df_sorted[existing_columns_to_show].head(20)

            day_info['headers'] = existing_columns_to_show
            day_info['data'] = df_display.to_dict('records')
        else:
            day_info['data'] = None
            day_info['headers'] = []
        
        daily_data.append(day_info)

    return render_template('show_data.html', daily_data=daily_data, current_market=market)

if __name__ == "__main__":
    StockDataCache()
    app.run(host='0.0.0.0', port=5000, debug=True)