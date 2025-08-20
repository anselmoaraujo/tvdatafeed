import pandas as pd
from tvDatafeed import TvDatafeed, Interval
import os
from dotenv import load_dotenv

def count_bars(start, end, freq):
    rng = pd.date_range(start=start, end=end, freq=freq)
    if len(rng) > 0 and rng[-1] == pd.to_datetime(end):
        return len(rng) - 1
    return len(rng)

def get_hist_chunk(tv, symbol, exchange, interval, n_bars):
    df = tv.get_hist(
        symbol=symbol,
        exchange=exchange,
        interval=interval,
        n_bars=n_bars
    )
    return df

def split_and_download(tv, symbol, exchange, interval, start, end, freq, max_bars=20000, adj_tz=False, conv_tz=False):
    start = pd.to_datetime(start)
    end = pd.to_datetime(end)
    all_data = []
    current_end = end

    while current_end > start:
        chunk_start = current_end - max_bars * pd.to_timedelta(freq)
        if chunk_start < start:
            chunk_start = start
        n_bars = count_bars(chunk_start, current_end, freq)
        df = get_hist_chunk(tv, symbol, exchange, interval, n_bars)
        if df is not None and not df.empty:
            # Filter to only include data within the chunk
            df = df[(df.index >= chunk_start) & (df.index < current_end)]
            all_data.append(df)
        current_end = chunk_start

    if all_data:
        final_df = pd.concat(reversed(all_data))
        final_df = final_df[(final_df.index >= start) & (final_df.index < end)]
        if adj_tz:
            # Adjust timezone: localize to UTC-3 (e.g., America/Sao_Paulo)
            final_df = final_df.reset_index()
            final_df['datetime'] = pd.to_datetime(final_df['datetime'])
            final_df['datetime'] = final_df['datetime'].dt.tz_localize('America/Sao_Paulo')
            # Optionally convert to UTC:
            if conv_tz:
                final_df['datetime'] = final_df['datetime'].dt.tz_convert('UTC')
            final_df = final_df.set_index('datetime')
            return final_df
        else:
            return final_df
    else:
        return pd.DataFrame()

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    def get_nonempty_input(prompt):
        while True:
            value = input(prompt).strip()
            if value:
                return value
            print("Input cannot be empty. Please try again.")

    symbol = get_nonempty_input("Enter symbol (e.g., BTC.D): ")
    exchange = get_nonempty_input("Enter exchange (e.g., CRYPTOCAP): ")
    interval_str = get_nonempty_input("Enter interval (e.g., 5min): ")
    start_date = get_nonempty_input("Enter start date (YYYY-MM-DD HH:MM:SS): ")
    end_date = get_nonempty_input("Enter end date (YYYY-MM-DD HH:MM:SS): ")
    adj_tz = get_nonempty_input("Adjust timezone to local? (yes/no): ").strip().lower() == "yes"
    conv_tz = get_nonempty_input("Convert timezone to UTC? (yes/no): ").strip().lower() == "yes"

    interval_map = {
        "1min": Interval.in_1_minute,
        "3min": Interval.in_3_minute,
        "5min": Interval.in_5_minute,
        "15min": Interval.in_15_minute,
        "30min": Interval.in_30_minute,
        "1h": Interval.in_1_hour,
        "4h": Interval.in_4_hour,
        "1d": Interval.in_daily,
        "1w": Interval.in_weekly,
        "1M": Interval.in_monthly,
    }
    if interval_str not in interval_map:
        print(f"Invalid interval: {interval_str}. Valid options: {list(interval_map.keys())}")
        exit(1)
    interval = interval_map[interval_str]
    
    # Load credentials from .env or prompt
    username = os.getenv("TV_USERNAME")
    password = os.getenv("TV_PASSWORD")
    if not username:
        username = input("Enter TradingView username (or leave blank for nologin): ").strip() or None
    if not password and username:
        password = input("Enter TradingView password: ").strip() or None

    # Use login if credentials provided, else fallback to nologin
    if username and password:
        tv = TvDatafeed(username, password)
    else:
        tv = TvDatafeed()

    final_data = split_and_download(tv, symbol, exchange, interval, start_date, end_date, interval_str, adj_tz=adj_tz, conv_tz=conv_tz)
    start_date_str = pd.to_datetime(start_date).strftime('%Y-%m-%d')
    end_date_str = pd.to_datetime(end_date).strftime('%Y-%m-%d')
    # Ensure the 'data' folder exists
    os.makedirs('data', exist_ok=True)
    fname = f"data/final_data_{symbol}_{start_date_str}_{end_date_str}_{interval_str}.csv"
    final_data.to_csv(fname)
    print(f"Data saved to {fname}")
