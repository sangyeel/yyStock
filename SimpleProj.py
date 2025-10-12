from pykrx import stock
import datetime as dt

def calDailyStockTurnOverRatio(workingDay,marketCapdf):
    print("Cal Daily Stock Turnover Ratio")
    #marketCapdf['종목명'] = marketCapdf.index.map(lambda x: stock.get_market_ticker_name(x))
    marketCapdf['일일회전율'] = marketCapdf.apply(lambda x: (x['거래량'] / x['상장주식수']) *100, axis=1)
    #marketCapdf = marketCapdf.sort_values(by='일일회전율', ascending=False)
    #marketCapdf.reset_index(inplace=True)
    return marketCapdf

def buildBasicStockData():
    workingDay = stock.get_nearest_business_day_in_a_week()
    print("Working Day:", workingDay)
    marketCapDf = stock.get_market_cap(workingDay,market="ALL")
    marketCapDf = calDailyStockTurnOverRatio(workingDay,marketCapDf)
    fundDf = stock.get_market_fundamental(workingDay,market="ALL")
    combinedDf = marketCapDf.join(fundDf, how='inner')
    combinedDf['종목명'] = combinedDf.index.map(lambda x: stock.get_market_ticker_name(x))
    combinedDf = combinedDf.sort_values(by='일일회전율', ascending=False)
    return combinedDf
    
if __name__ == "__main__":
    #test()
    basicInfoDf = buildBasicStockData()
    print(basicInfoDf.head(10))
    