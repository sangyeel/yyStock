# Flask 라이브러리가 설치되어 있지 않다면, 터미널에서 pip install Flask 를 실행해주세요.
# pykrx 라이브러리가 설치되어 있지 않다면, 터미널에서 pip install pykrx 를 실행해주세요.
from flask import Flask, request, render_template
from pykrx import stock
import datetime as dt

app = Flask(__name__)

def get_stock_data():
    # Define the date
    workingDay = stock.get_nearest_business_day_in_a_week()
    print("Working Day:", workingDay)

    # 1. Get turnover data
    turnover_df = stock.get_market_cap(workingDay, market="ALL")
    turnover_df['종목명'] = turnover_df.index.map(lambda x: stock.get_market_ticker_name(x))
    turnover_df['일일회전율'] = turnover_df.apply(lambda x: (x['거래량'] / x['상장주식수']) * 100 if x['상장주식수'] > 0 else 0, axis=1)
    
    # 2. Get fundamental data (PER, PBR)
    fundamental_df = stock.get_market_fundamental(workingDay, market="ALL")

    # 3. Combine the dataframes
    # Use an inner join to make sure we have all data for each stock
    combined_df = turnover_df.join(fundamental_df, how='inner')

    # Select and reorder columns for display
    display_cols = ['종목명', 'PER', 'PBR', '일일회전율', '시가', '고가', '저가']
    # Filter for columns that actually exist in the combined_df to prevent errors
    existing_cols = [col for col in display_cols if col in combined_df.columns]
    combined_df = combined_df[existing_cols]
    
    return combined_df

@app.route('/')
def show_table():
    # Get the sort parameter from the URL (e.g., /?sort=per_asc)
    sort_by = request.args.get('sort', default='turnover_desc', type=str)

    df = get_stock_data()

    # Sort the dataframe based on the parameter
    if sort_by == 'per_asc' and 'PER' in df.columns:
        df = df.sort_values(by='PER')
    elif sort_by == 'pbr_asc' and 'PBR' in df.columns:
        df = df.sort_values(by='PBR')
    elif sort_by == 'turnover_desc' and '일일회전율' in df.columns:
        df = df.sort_values(by='일일회전율', ascending=False)
    else:
        # Default sort
        if '일일회전율' in df.columns:
            df = df.sort_values(by='일일회전율', ascending=False)

    # Convert dataframe to HTML table, remove index
    html_table = df.to_html(index=False, classes='table', border=0)

    # Render the external HTML template, passing the table data into it
    return render_template('show_data.html', table_html=html_table)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=True)