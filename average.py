from multiprocessing import Pool
from pathlib import Path
import pandas as pd
import os
from time_series import Tests_before_data_processing

def read_file(file_path: str) -> pd.DataFrame:
    suffix = Path(file_path).suffix
    if suffix == ".csv":
        return pd.read_csv(file_path, parse_dates=["timestamp"])
    elif suffix == ".parquet":
        return pd.read_parquet(file_path)
    else:
        raise ValueError(f"Unsupported file format: {suffix}")

def write_file(df: pd.DataFrame, file_path: str):
    suffix = Path(file_path).suffix
    if suffix == ".csv":
        df.to_csv(file_path, index=False)
    elif suffix == ".parquet":
        df.to_parquet(file_path, index=False)
    else:
        raise ValueError(f"Unsupported file format: {suffix}")

def average_per_hour(file_path):
    df = Tests_before_data_processing(file_path)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors='coerce')
    df = df.set_index("timestamp")
    hourly_avg = df.resample("h").mean().dropna()
    print("Hourly average:")
    print(hourly_avg)

def avg_per_group(args):
    group, date, part_of_file, filename = args
    group = group.set_index("timestamp")
    hourly_avg = group.resample("h").mean().dropna()
    daily_file = os.path.join(part_of_file, f"{date}{filename.suffix}")
    write_file(hourly_avg.reset_index(), daily_file)

def average_per_hour_split(file_path, delete_after_merge=False):
    df = Tests_before_data_processing(file_path)

    filename = Path(file_path)
    part_of_file = "files/hourly_average"
    final_output = f"files/final_hourly_avg{filename.suffix}"

    os.makedirs(part_of_file, exist_ok=True)

    tasks = [(group, date, part_of_file, filename) for date, group in df.groupby(df["timestamp"].dt.date)]

    with Pool(os.cpu_count()) as pool:
        pool.map(avg_per_group, tasks)

    all_files = [os.path.join(part_of_file, f) for f in os.listdir(part_of_file) if f.endswith(filename.suffix)]
    df_list = [read_file(f) for f in all_files]
    final_df = pd.concat(df_list).sort_values("timestamp")
    write_file(final_df, final_output)

    if delete_after_merge:
        for f in all_files:
            os.remove(f)
        os.rmdir(part_of_file)

    return final_df

if __name__ == "__main__":
    average_per_hour("./files/time_series.csv")
    average_per_hour_split("./files/time_series.csv", delete_after_merge=True)
    average_per_hour_split("./files/time_series.parquet", delete_after_merge=True)
