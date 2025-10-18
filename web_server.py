# Flask 라이브러리가 설치되어 있지 않다면, 터미널에서 pip install Flask 를 실행해주세요.
# pykrx 라이브러리가 설치되어 있지 않다면, 터미널에서 pip install pykrx 를 실행해주세요.
from flask import Flask, request, render_template
import pandas as pd
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

                combined_df = combined_df[(combined_df['거래량'] != 0) & (combined_df['일일회전율'] != 0)]
                
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
    sort_by = request.args.get('sort', default='volume_desc', type=str)

    all_data, working_days = StockDataCache().get_data()
    data_dict = all_data.get(market, {})

    # 3일 연속 거래량 증가 종목 필터링 로직
    filtered_tickers = None
    if sort_by == 'volume_increase_3_days':
        filtered_tickers = []
        if len(working_days) >= 4:
            # working_days는 최신순으로 정렬되어 있음: ['오늘', '어제', '그제', ...]
            df_d0 = data_dict.get(working_days[0])
            df_d1 = data_dict.get(working_days[1])
            df_d2 = data_dict.get(working_days[2])
            df_d3 = data_dict.get(working_days[3])

            # 4일치 데이터가 모두 있고, '거래량' 컬럼이 있는지 확인
            if all(df is not None and not df.empty and '거래량' in df.columns for df in [df_d0, df_d1, df_d2, df_d3]):
                common_tickers = df_d0.index.intersection(df_d1.index).intersection(df_d2.index).intersection(df_d3.index)
                
                for ticker in common_tickers:
                    try:
                        vol0 = df_d0.loc[ticker, '거래량']
                        vol1 = df_d1.loc[ticker, '거래량']
                        vol2 = df_d2.loc[ticker, '거래량']
                        vol3 = df_d3.loc[ticker, '거래량']

                        # 0으로 나누는 것을 방지하고, 3일 연속 30% 이상 증가했는지 확인
                        if vol1 > 0 and vol2 > 0 and vol3 > 0:
                            if (vol0 / vol1 > 1.3) and (vol1 / vol2 > 1.3) and (vol2 / vol3 > 1.3):
                                filtered_tickers.append(ticker)
                    except (KeyError, ZeroDivisionError):
                        continue # 티커가 없거나 거래량이 0인 경우 건너뜀

    daily_data = []
    for day in working_days:
        df = data_dict.get(day)
        day_formatted = f"{day[0:4]}-{day[4:6]}-{day[6:8]}"
        day_info = {'date': day_formatted}

        if df is not None and not df.empty:
            
            df_display_source = df
            # 필터링된 종목이 있으면 해당 종목만 사용
            if filtered_tickers is not None:
                df_display_source = df[df.index.isin(filtered_tickers)]

            if df_display_source.empty:
                day_info['data'] = None
                day_info['headers'] = []
                daily_data.append(day_info)
                continue

            # 정렬 로직
            if sort_by == 'turnover_desc' and '일일회전율' in df_display_source.columns:
                df_sorted = df_display_source.sort_values(by='일일회전율', ascending=False)
            elif sort_by == 'turnover_asc' and '일일회전율' in df_display_source.columns:
                df_sorted = df_display_source.sort_values(by='일일회전율', ascending=True)
            elif sort_by == 'rate_desc' and '등락률' in df_display_source.columns:
                df_sorted = df_display_source.sort_values(by='등락률', ascending=False)
            elif sort_by == 'rate_asc' and '등락률' in df_display_source.columns:
                df_sorted = df_display_source.sort_values(by='등락률', ascending=True)
            elif sort_by == 'volume_asc' and '거래량' in df_display_source.columns:
                df_sorted = df_display_source.sort_values(by='거래량', ascending=True)
            else: # 'volume_desc' 또는 'volume_increase_3_days' (기본 정렬)
                df_sorted = df_display_source.sort_values(by='거래량', ascending=False)

            columns_to_show = ['종목명', '거래량', '일일회전율', '등락률', '종가']
            existing_columns_to_show = [col for col in columns_to_show if col in df_sorted.columns]
            df_display = df_sorted[existing_columns_to_show].head(20)

            day_info['headers'] = existing_columns_to_show
            day_info['data'] = df_display.to_dict('records')
        else:
            day_info['data'] = None
            day_info['headers'] = []
        
        daily_data.append(day_info)

    return render_template('show_data.html', daily_data=daily_data, current_market=market, current_sort=sort_by)

if __name__ == "__main__":
    StockDataCache()
    app.run(host='0.0.0.0', port=5000, debug=True)