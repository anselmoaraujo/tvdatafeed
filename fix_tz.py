import pandas as pd
import os

def convert_to_utc(filename, local_tz='America/Sao_Paulo'):
    filepath = os.path.join('data', filename)
    df = pd.read_csv(filepath)
    df['datetime'] = pd.to_datetime(df['datetime'])
    df['datetime'] = df['datetime'].dt.tz_localize(local_tz)
    df['datetime'] = df['datetime'].dt.tz_convert('UTC')
    df.to_csv(filepath, index=False)
    print(f"Timezone converted to UTC and saved to {filepath}")

if __name__ == "__main__":
    filename = input("Enter the filename in the data folder (e.g., final_data_BTC.D_2025-07-16_2025-08-20_5min.csv): ").strip()
    convert_to_utc(filename)
   
