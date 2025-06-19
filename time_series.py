import pandas as pd
from pathlib import Path

def Tests_before_data_processing(file_path):

    filename = Path(file_path)

    if filename.suffix == '.parquet':
        df = pd.read_parquet(file_path)  
    elif filename.suffix == '.csv':
        df = pd.read_csv(file_path) 


    if 'timestamp' not in df.columns or 'value' not in df.columns:
        print("Error: Columns are not correctly loaded.")
        return
    
    df['timestamp'] = df['timestamp'].astype(str).str.strip()
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce', dayfirst=True)
    df = df.dropna(subset=['timestamp'])
    df['value'] = pd.to_numeric(df['value'], errors="coerce")
    df.dropna(subset=['value'], inplace=True)
    df = df.drop_duplicates(subset=['timestamp','value'])
    return df

if __name__ == "__main__":
    Tests_before_data_processing("./files/time_series.csv")  


