import pandas as pd
from tvDatafeed import TvDatafeed, Interval
import os
from dotenv import load_dotenv

def calculate_bars_needed(start_date, end_date, interval_str):
    """Calculate how many bars we need for the given period"""
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    total_days = (end - start).days
    
    bars_per_day = {
        "1min": 1440,
        "3min": 480,
        "5min": 288,
        "15min": 96,
        "30min": 48,
        "1h": 24,
        "4h": 6,
        "1d": 1,
        "1w": 1/7,
        "1M": 1/30
    }
    
    estimated_bars = int(total_days * bars_per_day.get(interval_str, 288))
    return estimated_bars

def try_direct_download(tv, symbol, exchange, interval, start_date, end_date, interval_str):
    """Try to download all data in one go with progressively larger requests"""
    
    estimated_bars = calculate_bars_needed(start_date, end_date, interval_str)
    print(f"Estimated bars needed: {estimated_bars:,}")
    
    # Try different bar counts, starting with estimated and going up
    bar_requests = [
        estimated_bars,
        estimated_bars + 10000,
        50000,
        100000,
        200000,
        500000,
        1000000  # Go big!
    ]
    
    for n_bars in bar_requests:
        print(f"\nTrying to get {n_bars:,} bars...")
        
        try:
            df = tv.get_hist(
                symbol=symbol,
                exchange=exchange,
                interval=interval,
                n_bars=n_bars
            )
            
            if df is not None and not df.empty:
                print(f"✓ SUCCESS! Got {len(df):,} bars")
                print(f"  Date range: {df.index.min()} to {df.index.max()}")
                
                # Filter to our desired date range
                start_dt = pd.to_datetime(start_date)
                end_dt = pd.to_datetime(end_date)
                
                df_filtered = df[(df.index >= start_dt) & (df.index <= end_dt)]
                print(f"  Filtered to {len(df_filtered):,} bars in desired range")
                
                if len(df_filtered) > 0:
                    return df_filtered
                else:
                    print("  ⚠ No data in the requested date range")
            else:
                print(f"  ✗ No data returned")
                
        except Exception as e:
            print(f"  ✗ Error: {e}")
    
    return pd.DataFrame()

def try_alternative_symbols(tv, base_symbol, exchanges, interval, start_date, end_date, interval_str):
    """Try alternative symbol/exchange combinations"""
    
    # Common alternatives for Bitcoin Dominance
    alternatives = [
        (base_symbol, "CRYPTOCAP"),
        ("BTCDOM", "CRYPTOCAP"),
        ("BTC.D", "CRYPTOCAP"),
        ("BTCDOMUSDT", "BINANCE"),
        ("BTCDOM", "BYBIT"),
        # Test with a known working symbol first
        ("BTCUSDT", "BINANCE"),
        ("BTC", "BITSTAMP"),
    ]
    
    for symbol, exchange in alternatives:
        print(f"\n{'='*50}")
        print(f"Trying: {symbol} on {exchange}")
        print(f"{'='*50}")
        
        result = try_direct_download(tv, symbol, exchange, interval, start_date, end_date, interval_str)
        
        if not result.empty:
            print(f"✅ SUCCESS with {symbol} on {exchange}!")
            return result, symbol, exchange
        else:
            print(f"❌ Failed with {symbol} on {exchange}")
    
    return pd.DataFrame(), None, None

def apply_timezone_adjustments(df, adj_tz=False, conv_tz=False):
    """Apply timezone adjustments to the dataframe"""
    if not adj_tz or df.empty:
        return df
        
    df_copy = df.copy()
    df_copy = df_copy.reset_index()
    df_copy['datetime'] = pd.to_datetime(df_copy['datetime'])
    
    try:
        # Localize to São Paulo timezone  
        df_copy['datetime'] = df_copy['datetime'].dt.tz_localize('America/Sao_Paulo')
        print("✓ Applied São Paulo timezone")
        
        # Optionally convert to UTC
        if conv_tz:
            df_copy['datetime'] = df_copy['datetime'].dt.tz_convert('UTC')
            print("✓ Converted to UTC")
    except Exception as e:
        print(f"⚠ Timezone adjustment failed: {e}")
        # If timezone fails, just return the data without timezone info
        pass
    
    df_copy = df_copy.set_index('datetime')
    return df_copy

if __name__ == "__main__":
    load_dotenv()

    def get_nonempty_input(prompt):
        while True:
            value = input(prompt).strip()
            if value:
                return value
            print("Input cannot be empty. Please try again.")

    print("🚀 SIMPLE & DIRECT TradingView Data Downloader")
    print("=" * 60)
    
    # Get user input
    symbol = get_nonempty_input("Enter symbol (e.g., BTC.D): ")
    exchange = get_nonempty_input("Enter exchange (e.g., CRYPTOCAP): ")
    interval_str = get_nonempty_input("Enter interval (5min/15min/1h/4h/1d): ")
    start_date = get_nonempty_input("Enter start date (YYYY-MM-DD): ") + " 00:00:00"
    end_date = get_nonempty_input("Enter end date (YYYY-MM-DD): ") + " 23:59:59"
    
    adj_tz = get_nonempty_input("Adjust timezone to São Paulo? (yes/no): ").strip().lower() == "yes"
    conv_tz = False
    if adj_tz:
        conv_tz = get_nonempty_input("Also convert to UTC? (yes/no): ").strip().lower() == "yes"
    
    try_alternatives = get_nonempty_input("Try alternative symbols if main fails? (yes/no): ").strip().lower() == "yes"

    # Map intervals
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
        print(f"❌ Invalid interval. Valid options: {list(interval_map.keys())}")
        exit(1)
        
    interval = interval_map[interval_str]
    
    # Get credentials
    username = os.getenv("TV_USERNAME")
    password = os.getenv("TV_PASSWORD")
    
    if not username:
        username = input("Enter TradingView username: ").strip()
    if not password:
        password = input("Enter TradingView password: ").strip()

    print(f"\n🔐 Connecting to TradingView...")
    if username and password:
        tv = TvDatafeed(username, password)
        print(f"✓ Connected as: {username}")
    else:
        tv = TvDatafeed()
        print("✓ Connected anonymously (limited data)")

    # Try to get the data
    print(f"\n📊 Requesting data: {symbol} on {exchange}")
    print(f"📅 Period: {start_date} to {end_date}")
    print(f"⏱️ Interval: {interval_str}")
    
    final_data = try_direct_download(tv, symbol, exchange, interval, start_date, end_date, interval_str)
    used_symbol = symbol
    used_exchange = exchange
    
    # If main symbol failed and user wants alternatives
    if final_data.empty and try_alternatives:
        print(f"\n🔄 Main symbol failed, trying alternatives...")
        final_data, used_symbol, used_exchange = try_alternative_symbols(
            tv, symbol, [exchange], interval, start_date, end_date, interval_str
        )
    
    if not final_data.empty:
        print(f"\n🎉 DATA RETRIEVED SUCCESSFULLY!")
        print(f"📈 Symbol used: {used_symbol} on {used_exchange}")
        print(f"📊 Records: {len(final_data):,}")
        print(f"📅 Actual range: {final_data.index.min()} to {final_data.index.max()}")
        
        # Apply timezone adjustments
        if adj_tz:
            print(f"\n🌍 Applying timezone adjustments...")
            final_data = apply_timezone_adjustments(final_data, adj_tz, conv_tz)
        
        # Save data
        start_str = pd.to_datetime(start_date).strftime('%Y-%m-%d')
        end_str = pd.to_datetime(end_date).strftime('%Y-%m-%d')
        
        os.makedirs('data', exist_ok=True)
        filename = f"data/{used_symbol}_{start_str}_to_{end_str}_{interval_str}.csv"
        final_data.to_csv(filename)
        
        print(f"\n💾 File saved: {filename}")
        print(f"\n📋 Sample data:")
        print(final_data.head())
        
        # Data quality check
        print(f"\n🔍 Data Quality Check:")
        print(f"   Missing values: {final_data.isnull().sum().sum()}")
        print(f"   Duplicate timestamps: {final_data.index.duplicated().sum()}")
        print(f"   Data columns: {list(final_data.columns)}")
        
    else:
        print(f"\n❌ FAILED TO RETRIEVE DATA")
        print(f"\n🔧 Troubleshooting suggestions:")
        print(f"   1. Verify your TradingView account has data access for this symbol")
        print(f"   2. Check if {symbol} on {exchange} is the correct combination")
        print(f"   3. Try a larger interval (e.g., 1h instead of 5min)")
        print(f"   4. Try a shorter date range (e.g., last 6 months)")
        print(f"   5. Verify your credentials work on tradingview.com")
        
        # Quick test with known working symbol
        print(f"\n🧪 Quick test with BTCUSDT...")
        test_data = try_direct_download(tv, "BTCUSDT", "BINANCE", interval, 
                                      "2024-08-01 00:00:00", end_date, interval_str)
        if not test_data.empty:
            print(f"✅ BTCUSDT works - your connection is fine, issue is with {symbol}")
        else:
            print(f"❌ Even BTCUSDT failed - likely an account/connection issue")
