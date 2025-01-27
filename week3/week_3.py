# accept start & end hour as arguments
# perform:
    # rank colors by distinct users -> english
    # calc avg session length
    # pixel placement percentiles (50th, 75th, 90th, and 99th)
    # count first time users

# processed data < 3GB
# calc tasks run < 30 sec

# imports
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import sys
import time
import numpy as np

# preprocess data function (use chunks and write incrementally to parquet)
def preprocess_data(start_time, end_time):

    start_time = pd.to_datetime(start_time)  # should be already in UTC
    end_time = pd.to_datetime(end_time)

    parquet_file = 'processed_canvas_history.parquet'
    parquet_writer = None  # initialize writer as None

    # def schema explicitly for parquet
    initial_schema = pa.schema([
        ('timestamp', pa.timestamp('ns', tz='UTC')),
        ('user_id', pa.string()),
        ('pixel_color', pa.string()),
        ('coordinate', pa.string()),
        ('__index_level_0__', pa.int64())  # index column?
    ])

    for i, chunk in enumerate(pd.read_csv('./2022_place_canvas_history.csv', chunksize=1_000_000)):
        
        chunk = chunk[['timestamp', 'user_id', 'pixel_color', 'coordinate']]  # only relevant columns

        chunk['timestamp'] = pd.to_datetime(chunk['timestamp'], errors='coerce')
        chunk = chunk[(chunk['timestamp'] >= start_time) & (chunk['timestamp'] <= end_time)]

        chunk['pixel_color'] = chunk['pixel_color'].map({
            '#FFFFFF': 'White', '#000000': 'Black', '#FF0000': 'Red', 
            '#00FF00': 'Green', '#0000FF': 'Blue', '#FFFF00': 'Yellow'  # convert hex codes to names
        }).fillna(chunk['pixel_color'])  # keep original hex if not mapped

        chunk['pixel_color'] = chunk['pixel_color'].astype(str) # ensure consistent schema for pixel_color
        chunk['__index_level_0__'] = chunk.index  # add index as column

        table = pa.Table.from_pandas(chunk, schema=initial_schema) # convert to pyarrow table

        # write data incrementally to parquet
        if parquet_writer is None:
            parquet_writer = pq.ParquetWriter(parquet_file, schema=initial_schema, compression='snappy')

        parquet_writer.write_table(table)
        print(f"Chunk {i + 1} processed and written to Parquet.")

    if parquet_writer:
        parquet_writer.close()  # close Parquet writer
        print(f"Data successfully written to {parquet_file}.")

    df_parquet = pd.read_parquet(parquet_file)
    return df_parquet  # return as df



# compute tasks

# rank colors by distinct users function
def rank_colors_distinct_users(df):

    color_rank = df.groupby('pixel_color')['user_id'].nunique().sort_values(ascending=False) # groupby color (pixel_color), count num of users (user_id)
    return color_rank


# calc avg session length function
def calc_avg_session_length(df):

    df = df.sort_values(['user_id', 'timestamp']) # sort
    df['time_diff'] = df['timestamp'].diff() # calc time diff
    df['time_diff'] = df['time_diff'].where(df['user_id'] == df['user_id'].shift()) # et time_diff to NaT for rows where user_id changes
    valid_sessions = df['time_diff'][df['time_diff'] <= pd.Timedelta(minutes=15)] # filter out sessions longer than 15 min
    session_lengths = valid_sessions.dt.total_seconds() # convert to sec
    
    return session_lengths.mean() if not session_lengths.empty else 0 # calc mean


# pixel placement percentiles function
def calc_pixel_percentile(df):

    # count number of pixels each user has placed (groupby user_id)
    # use np.percentile 

    pixel_counts = df.groupby('user_id')['pixel_color'].count()
    percentiles = np.percentile(pixel_counts, [50, 75, 90, 99])

    output = (
        f"50: {int(percentiles[0])} pixels\n"
        f"75: {int(percentiles[1])} pixels\n"
        f"90: {int(percentiles[2])} pixels\n"
        f"99: {int(percentiles[3])} pixels\n"
    )

    return output


# count first time users
def count_first_time_users(df):

    # for each user in given timeframe, check if any appearnace of that user before timestamp
    # if no appearance, increment count of first_time_user

    # first_time_user_count = 0
    # df_sorted = df.sort_values(by=['user_id', 'timestamp'])
    # seen_users_before = set()

    # for i, row in df_sorted.iterrows():
    #     if row['user_id'] not in seen_users_before:
    #         first_time_user_count += 1
    #         seen_users_before.add(row['user_id'])

    # return first_time_user_count
    return df['user_id'].nunique() # returned the same result, but faster in this case?



def main():
    try:
        start_time = pd.to_datetime(sys.argv[1], format="%Y-%m-%d %H").tz_localize('UTC')
        end_time = pd.to_datetime(sys.argv[2], format="%Y-%m-%d %H").tz_localize('UTC')

        if start_time >= end_time:
            print("Error: End hour must be after start hour.")
            sys.exit(1)

    except ValueError:
        print("Error: Incorrect Date Format.")
        sys.exit(1)

    df = preprocess_data(start_time, end_time)

    # parquet_file = './processed_canvas_history.parquet'
    # df = pd.read_parquet(parquet_file) # just call directly from parquet file now (save time for debugging)

    start = time.perf_counter_ns() # start execution time

    # color ranking
    color_start = time.perf_counter_ns()
    color_rank = rank_colors_distinct_users(df)
    color_end = time.perf_counter_ns()

    # session length calculation
    session_start = time.perf_counter_ns()
    avg_session_length = calc_avg_session_length(df)
    session_end = time.perf_counter_ns()

    # pixel percentiles
    pixel_start = time.perf_counter_ns()
    pixel_percentiles = calc_pixel_percentile(df)
    pixel_end = time.perf_counter_ns()

    # first-time user count
    first_time_start = time.perf_counter_ns()
    first_time_user_count = count_first_time_users(df)
    first_time_end = time.perf_counter_ns()

    end = time.perf_counter_ns()
    exec_time = (end - start) / 1_000_000 # convert from ns to ms

    # print time for each task
    print(f"Color Rank Time: {(color_end - color_start) / 1_000_000:.2f} ms")
    print(f"Session Length Time: {(session_end - session_start) / 1_000_000:.2f} ms")
    print(f"Pixel Percentiles Time: {(pixel_end - pixel_start) / 1_000_000:.2f} ms")
    print(f"First-Time User Count Time: {(first_time_end - first_time_start) / 1_000_000:.2f} ms")

    # print results
    print(f"Ranking of Colors by Distinct Users:\n{color_rank}")
    print(f"\nAverage Session Length: {avg_session_length:.2f} seconds")
    print(f"\nPercentiles of Pixels Placed:\n{pixel_percentiles}")
    print(f"\nCount of First-Time Users: {first_time_user_count}")
    print(f"\nRuntime: {exec_time:.0f} ms")


if __name__=="__main__":
    main()