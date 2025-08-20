from tvDatafeed import TvDatafeed, Interval

# Create a TvDatafeed instance (add username/password if needed)
tv = TvDatafeed()

# Retrieve 5-min bars for BTC.D from TradingView
data = tv.get_hist(
    symbol='BTC.D',
    exchange='CRYPTOCAP',  # BTC.D is usually under CRYPTOCAP
    interval=Interval.in_5_minute,
    n_bars=5000  # max bars per request; adjust as needed
)

# Filter for data since 2025-07-16
data = data[data.index >= '2025-07-16']

print(data)
