import pandas as pd

def count_bars(start, end, freq):
    """
    Calculate the number of bars between two dates for a given frequency.

    Parameters:
        start (str or pd.Timestamp): Start date/time (inclusive)
        end (str or pd.Timestamp): End date/time (exclusive)
        freq (str): Frequency string, e.g., '5min', '1H', '1D'

    Returns:
        int: Number of bars
    """
    rng = pd.date_range(start=start, end=end, freq=freq)
    # Exclude the last bar if it matches the end exactly
    if len(rng) > 0 and rng[-1] == pd.to_datetime(end):
        return len(rng) - 1
    return len(rng)

if __name__ == "__main__":
    # Example usage
    start_date = "2025-07-16 00:00:00"
    end_date = "2025-08-20 13:00:00"
    frequency = "5min"
    num_bars = count_bars(start=start_date, end=end_date, freq=frequency)
    print(f"Number of {frequency} bars between {start_date} and {end_date}: {num_bars}")
