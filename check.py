import pandas as pd

df = pd.read_csv('final_data.csv')

# Check the column names
print(df.columns)

# Check the first few rows
print(df.head())

# Check the dtype of the datetime column
print(df['datetime'].dtype)

# If it's a string/object, parse it as datetime
df['datetime'] = pd.to_datetime(df['datetime'])

# Check if the datetime column is timezone-aware
print(df['datetime'].dt.tz)
