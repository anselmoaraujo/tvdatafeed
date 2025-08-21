from tvDatafeed import TvDatafeed, Interval
import pandas as pd
from datetime import datetime, timedelta
import math
import os
from dotenv import load_dotenv

class TvDatafeedTimeRange(TvDatafeed):
    """Extended TvDatafeed class with time-range support"""
    
    def __init__(self, username=None, password=None, data_folder='data'):
        super().__init__(username, password)
        
        # Set up data folder
        self.data_folder = data_folder
        self._create_data_folder()
        
        # Define approximate bars per day for different intervals
        self.bars_per_day = {
            Interval.in_1_minute: 1440,
            Interval.in_3_minute: 480,
            Interval.in_5_minute: 288,
            Interval.in_15_minute: 96,
            Interval.in_30_minute: 48,
            Interval.in_45_minute: 32,
            Interval.in_1_hour: 24,
            Interval.in_2_hour: 12,
            Interval.in_3_hour: 8,
            Interval.in_4_hour: 6,
            Interval.in_daily: 1,
            Interval.in_weekly: 1/7,
            Interval.in_monthly: 1/30
        }
    
    def _create_data_folder(self):
        """Create data folder if it doesn't exist"""
        if not os.path.exists(self.data_folder):
            os.makedirs(self.data_folder)
            print(f"Created data folder: {self.data_folder}")
        else:
            print(f"Using existing data folder: {self.data_folder}")
    
    def _generate_filename(self, symbol, exchange, interval, start_date, end_date):
        """Generate a standardized filename for the CSV file"""
        # Convert dates to string format
        if isinstance(start_date, datetime):
            start_str = start_date.strftime('%Y-%m-%d')
        else:
            start_str = str(start_date)
            
        if isinstance(end_date, datetime):
            end_str = end_date.strftime('%Y-%m-%d')
        else:
            end_str = str(end_date)
        
        # Create a clean interval name
        interval_name = interval.name.replace('in_', '').replace('_', '')
        
        # Generate filename
        filename = f"{exchange}_{symbol}_{interval_name}_{start_str}_to_{end_str}.csv"
        
        # Remove any invalid characters for filename
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        
        return os.path.join(self.data_folder, filename)
    
    def get_hist_by_date_range(self, symbol, exchange, interval, start_date, end_date, 
                             fut_contract=None, extended_session=False, save_csv=True, 
                             custom_filename=None):
        """
        Retrieve historical data for a specific date range and save to CSV
        
        Parameters:
        - symbol: Trading symbol (e.g., 'BTC.D')
        - exchange: Exchange name (e.g., 'CRYPTOCAP')
        - interval: Time interval (Interval enum)
        - start_date: Start date (datetime object or string 'YYYY-MM-DD')
        - end_date: End date (datetime object or string 'YYYY-MM-DD')
        - fut_contract: Futures contract (optional)
        - extended_session: Extended session flag
        - save_csv: Whether to save the data to CSV (default: True)
        - custom_filename: Custom filename for the CSV file (optional)
        
        Returns:
        - Tuple: (Pandas DataFrame with filtered data, CSV file path)
        """
        
        # Convert string dates to datetime objects if needed
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
        
        # Calculate the number of days in the range
        date_diff = (end_date - start_date).days
        
        # Estimate the number of bars needed
        bars_per_day = self.bars_per_day.get(interval, 1)
        estimated_bars = math.ceil(date_diff * bars_per_day * 1.2)  # Add 20% buffer
        
        # Ensure we don't exceed the maximum limit (5000 bars)
        estimated_bars = min(estimated_bars, 5000)
        
        # If estimated bars is too small, set a minimum
        estimated_bars = max(estimated_bars, 100)
        
        print(f"Fetching approximately {estimated_bars} bars for {date_diff} days...")
        
        try:
            # Get historical data using the original method
            data = self.get_hist(
                symbol=symbol, 
                exchange=exchange, 
                interval=interval, 
                n_bars=estimated_bars,
                fut_contract=fut_contract,
                extended_session=extended_session
            )
            
            if data is None or data.empty:
                print("No data retrieved from TradingView")
                return None, None
            
            # Filter data to the exact date range
            # Convert start_date and end_date to the same timezone as the data
            start_date_tz = pd.Timestamp(start_date)
            end_date_tz = pd.Timestamp(end_date)
            
            if data.index.tz is not None:
                start_date_tz = start_date_tz.tz_localize(data.index.tz)
                end_date_tz = end_date_tz.tz_localize(data.index.tz)
            
            filtered_data = data[(data.index >= start_date_tz) & (data.index <= end_date_tz)]
            
            print(f"Retrieved {len(data)} total bars, filtered to {len(filtered_data)} bars in date range")
            
            # Save to CSV if requested
            csv_path = None
            if save_csv and not filtered_data.empty:
                if custom_filename:
                    csv_path = os.path.join(self.data_folder, custom_filename)
                    if not csv_path.endswith('.csv'):
                        csv_path += '.csv'
                else:
                    csv_path = self._generate_filename(symbol, exchange, interval, start_date, end_date)
                
                # Save the data
                filtered_data.to_csv(csv_path)
                print(f"Data saved to: {csv_path}")
                
                # Display file info
                file_size = os.path.getsize(csv_path)
                print(f"File size: {file_size / 1024:.2f} KB")
            
            return filtered_data, csv_path
            
        except Exception as e:
            print(f"Error retrieving data: {e}")
            return None, None
    
    def get_hist_large_date_range(self, symbol, exchange, interval, start_date, end_date, 
                                chunk_days=30, save_csv=True, custom_filename=None):
        """
        Retrieve historical data for large date ranges by chunking requests
        """
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
        
        all_data = []
        current_date = start_date
        
        print(f"Processing large date range from {start_date.date()} to {end_date.date()}")
        print("This will be done in chunks to avoid API limits...")
        
        while current_date < end_date:
            chunk_end = min(current_date + timedelta(days=chunk_days), end_date)
            
            print(f"Fetching chunk: {current_date.date()} to {chunk_end.date()}")
            
            chunk_data, _ = self.get_hist_by_date_range(
                symbol=symbol,
                exchange=exchange, 
                interval=interval,
                start_date=current_date,
                end_date=chunk_end,
                save_csv=False  # Don't save individual chunks
            )
            
            if chunk_data is not None and not chunk_data.empty:
                all_data.append(chunk_data)
            
            current_date = chunk_end + timedelta(days=1)
        
        if all_data:
            # Combine all chunks and remove duplicates
            combined_data = pd.concat(all_data)
            combined_data = combined_data[~combined_data.index.duplicated(keep='first')]
            combined_data = combined_data.sort_index()
            
            # Save combined data to CSV if requested
            csv_path = None
            if save_csv:
                if custom_filename:
                    csv_path = os.path.join(self.data_folder, custom_filename)
                    if not csv_path.endswith('.csv'):
                        csv_path += '.csv'
                else:
                    csv_path = self._generate_filename(symbol, exchange, interval, start_date, end_date)
                
                combined_data.to_csv(csv_path)
                print(f"Combined data saved to: {csv_path}")
                
                # Display file info
                file_size = os.path.getsize(csv_path)
                print(f"File size: {file_size / 1024:.2f} KB")
                print(f"Total rows: {len(combined_data)}")
            
            return combined_data, csv_path
        
        return None, None

# Usage Example for CRYPTOCAP:BTC.D
def main():
    # Initialize with your TradingView credentials
    username = os.getenv("TV_USERNAME")
    password = os.getenv("TV_PASSWORD")
    
    if not username:
        username = input("Enter TradingView username: ").strip()
    if not password:
        password = input("Enter TradingView password: ").strip()

    print(f"\n🔐 Connecting to TradingView...")
    if username and password:
        tv = TvDatafeedTimeRange(username, password, data_folder='data')
        print(f"✓ Connected as: {username}")
    else:
        tv = TvDatafeedTimeRange(data_folder='data')
        print("✓ Connected anonymously (limited data)")
    
    # Define your parameters
    symbol = 'BTC.D'
    exchange = 'CRYPTOCAP'
    interval = Interval.in_5_minute  # You can choose any supported interval
    start_date = '2025-07-17'      # Start date
    end_date = '2025-08-11'        # End date

    print("=== Retrieving BTC Dominance Data ===")
    print(f"Symbol: {exchange}:{symbol}")
    print(f"Interval: {interval.name}")
    print(f"Date Range: {start_date} to {end_date}")
    print("-" * 50)
    
    # Retrieve data for the specified date range
    btc_dominance_data, csv_file_path = tv.get_hist_by_date_range(
        symbol=symbol,
        exchange=exchange,
        interval=interval,
        start_date=start_date,
        end_date=end_date,
        save_csv=True,
        # custom_filename='btc_dominance_custom.csv'  # Optional custom filename
    )
    
    if btc_dominance_data is not None:
        print(f"\n=== Data Summary ===")
        print(f"Data shape: {btc_dominance_data.shape}")
        print(f"Date range: {btc_dominance_data.index.min()} to {btc_dominance_data.index.max()}")
        print(f"CSV file saved at: {csv_file_path}")
        
        print(f"\n=== Statistical Summary ===")
        print(f"BTC Dominance Statistics:")
        print(f"Current: {btc_dominance_data['close'].iloc[-1]:.2f}%")
        print(f"Average: {btc_dominance_data['close'].mean():.2f}%")
        print(f"Min: {btc_dominance_data['close'].min():.2f}%")
        print(f"Max: {btc_dominance_data['close'].max():.2f}%")
        
        print(f"\n=== First 5 rows ===")
        print(btc_dominance_data.head())
        
        print(f"\n=== Last 5 rows ===")
        print(btc_dominance_data.tail())
        
        # Example: Create additional processed files
        daily_summary = btc_dominance_data.resample('D').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        daily_csv_path = os.path.join('data', f'{exchange}_{symbol}_daily_summary_{start_date}_to_{end_date}.csv')
        daily_summary.to_csv(daily_csv_path)
        print(f"\nDaily summary also saved to: {daily_csv_path}")
        
    else:
        print("Failed to retrieve data. Please check your credentials and parameters.")

# Example for large date ranges
def example_large_range():
    """Example for retrieving data over a very large date range"""
    
    username = 'your_tradingview_username'
    password = 'your_tradingview_password'
    
    tv = TvDatafeedTimeRange(username, password, data_folder='data')
    
    # Large date range example (2+ years)
    btc_data, csv_path = tv.get_hist_large_date_range(
        symbol='BTC.D',
        exchange='CRYPTOCAP',
        interval=Interval.in_5_minute,
        start_date='2022-01-01',
        end_date='2025-08-21',
        chunk_days=90,  # Process 90 days at a time
        custom_filename='btc_dominance_3_years.csv'
    )
    
    if btc_data is not None:
        print(f"Large dataset retrieved: {btc_data.shape}")
        print(f"Saved to: {csv_path}")

if __name__ == "__main__":
    main()
    
    # Uncomment the line below to run the large range example
    # example_large_range()
