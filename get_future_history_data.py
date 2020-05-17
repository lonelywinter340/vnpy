from ib_insync import *
import sys
import datetime
import pandas as pd
import numpy as np
# util.startLoop()  # uncomment this line when in a notebook

exchange_dict = {
    'QM': 'NYMEX',
    'NQ': 'GLOBEX',
    'MGC': 'COMEX',
    'BTC': 'CME',
}

#PORT = 7496
PORT = 4002

ib = IB()
ib.setTimeout(300)
ib.connect('127.0.0.1', PORT, clientId=np.random.randint(100))

code = sys.argv[1]
exchange = "GLOBEX" if code not in exchange_dict else exchange_dict[code]
rth = False
currency = "USD"
bar_size = "15 mins"

start_time = '20100101 05:00:00'
end_time = '20190606 05:00:00'

start_time_dt = datetime.datetime.strptime(start_time, '%Y%m%d %H:%M:%S')
end_time_dt = datetime.datetime.strptime(end_time, '%Y%m%d %H:%M:%S')
print(end_time_dt)
contract = ContFuture(code, exchange=exchange, currency=currency)

current_end_time = end_time_dt
dfs = []
while True:
    next_time = current_end_time - datetime.timedelta(days=30)
    next_time_str = datetime.datetime.strftime(next_time, '%Y%m%d %H:%M:%S')
    current_end_time_str = datetime.datetime.strftime(current_end_time, '%Y%m%d %H:%M:%S')
    print('Request data {}'.format(current_end_time))
    bars = ib.reqHistoricalData(contract, endDateTime=current_end_time, durationStr='32 D',
            barSizeSetting=bar_size, whatToShow='TRADES', useRTH=rth, formatDate=True)
    # convert to pandas dataframe:
    df = util.df(bars)
    out_df = df[['date', 'open', 'high', 'low', 'close', 'volume']]
    out_df.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
    out_df = out_df.set_index('datetime')
    out_df = out_df.loc[next_time_str: current_end_time_str, :]
    print(out_df)
    out_df.to_csv("{}/{}-{}-{}-{}-{}-{}.csv".format(code, code, bar_size, exchange, currency,
         next_time_str.replace(' ', '_'), current_end_time_str.replace(' ', '_')))
    #dfs.insert(0, out_df)
    #time = out_df['datetime'].values[0].astype(datetime.datetime) / (10 ** 9)
    #next_time = datetime.datetime.fromtimestamp(time) - datetime.timedelta(hours=8)
    #current_end_time = datetime.datetime.strftime(next_time, '%Y%m%d %H:%M:%S')
    current_end_time = next_time
    if next_time < start_time_dt:
        break

#print('Merge dataframes and save file')
#merged_df = pd.concat(dfs)
#print(merged_df)
#merged_df.to_csv("{}-{}-{}-{}-{}.csv".format(code, bar_size, exchange, currency, end_time.replace(' ', '_')))
