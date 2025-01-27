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

# preprocess data function (use chunks and write incrementally to Parquet)
def preprocess_data(start_time, end_time):

    start_time = pd.to_datetime(start_time).tz_localize('UTC') # convert (already in UTC)
    end_time = pd.to_datetime(end_time).tz_localize('UTC')

    parquet_file = 'processed_canvas_history.parquet'
    parquet_writer = None  # intialize writer as None

    # def schema explicitly for parquet file
    initial_schema = pa.schema([
        ('timestamp', pa.timestamp('ns', tz='UTC')),
        ('user_id', pa.string()),
        ('pixel_color', pa.string()),
        ('coordinate', pa.string()),
        ('__index_level_0__', pa.int64())  # index column?
    ])

    for i, chunk in enumerate(pd.read_csv('./2022_place_canvas_history.csv', chunksize=1_000_000)):
        chunk = chunk[['timestamp', 'user_id', 'pixel_color', 'coordinate']]  # only relevant cols

        chunk['timestamp'] = pd.to_datetime(chunk['timestamp'], errors='coerce') # convert to datetime
        chunk = chunk[(chunk['timestamp'] >= start_time) & (chunk['timestamp'] <= end_time)] # filter btwn start & end time

        chunk['pixel_color'] = chunk['pixel_color'].map({
            '#FFFFFF': 'White', '#000000': 'Black', '#FF0000': 'Red', 
            '#00FF00': 'Green', '#0000FF': 'Blue', '#FFFF00': 'Yellow' # convert hex codes to plain english
        }).fillna(chunk['pixel_color'])  # keep og hex if mapping not def

        chunk['pixel_color'] = chunk['pixel_color'].astype(str)  # ensure consistent schema
        chunk['__index_level_0__'] = chunk.index  # add index as col?

        table = pa.Table.from_pandas(chunk, schema=initial_schema) # convert to pyarrow table

        # write data incrementally
        if parquet_writer is None:
            parquet_writer = pq.ParquetWriter(parquet_file, schema=initial_schema, compression='snappy')

        parquet_writer.write_table(table)

        print(f"Chunk {i + 1} processed and written to Parquet.")

    if parquet_writer:
        parquet_writer.close() # close parquet writer
        print(f"Data successfully written to {parquet_file}.")

    return pd.read_parquet(parquet_file) # read & return as df



# compute tasks

# rank colors by distinct users function
def rank_colors_distinct_users(df):

    color_rank = df.groupby('pixel_color')['user_id'].nunique().sort_values(ascending=False) # groupby color (pixel_color), count num of users (user_id)
    return color_rank


# calc avg session length function
def calc_avg_session_length(df):

    # NEED TO ADD 15 min session?

    session_lengths = []

    for user_id, group in df.groupby('user_id'):

        group = group.sort_values('timestamp') # sort time for each user

        if len(group) > 1: # if user >1 pixel placement

            session_durations = group['timestamp'].diff().dt.total_seconds() # find time difference for each user
            session_lengths.extend(session_durations) # add each users duration to session lengths
    
    return np.mean(session_lengths) if session_lengths else 0 # avg


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

    first_time_user_count = 0
    df_sorted = df.sort_values(by=['user_id', 'timestamp'])
    seen_users_before = set()

    for i, row in df_sorted.iterrows():
        if row['user_id'] not in seen_users_before:
            first_time_user_count += 1
            seen_users_before.add(row['user_id'])

    return first_time_user_count



def main():

    try:
        start_time = datetime.strptime(sys.argv[1], "%Y-%m-%d %H") # accept time in YYYY-MM-DD HH format
        end_time = datetime.strptime(sys.argv[2], "%Y-%m-%d %H")

        if start_time >= end_time:
            print("Error: End hour must be after start hour.")
            sys.exit(1)

    except ValueError:
        print("Error: Incorrect Date Format.")
        sys.exit(1)

    df = preprocess_data(start_time, end_time)

    # df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S.%f UTC') # convert timestamp to format
    # df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]

    start = time.perf_counter_ns() # start execution time

    # run tasks
    color_rank = rank_colors_distinct_users(df)
    avg_session_length = calc_avg_session_length(df)
    pixel_percentiles = calc_pixel_percentile(df)
    first_time_user_count = count_first_time_users(df)

    end = time.perf_counter_ns()
    exec_time = (end - start) / 1_000_000 # convert from ns to ms

    # print results
    print(f"Ranking of Colors by Distinct Users:\n{color_rank}")
    print(f"\nAverage Session Length: {avg_session_length:.2f} seconds")
    print(f"\nPercentiles of Pixels Placed:\n{pixel_percentiles}")
    print(f"\nCount of First-Time Users: {first_time_user_count}")
    print(f"\nRuntime: {exec_time:.0f} ms")


if __name__=="__main__":
    main()