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

# preprocess data function (use chunks)
def preprocess_data():

    parquet_file = 'processed_canvas_history.parquet'
    all_chunks = []

    for i, chunk in enumerate(pd.read_csv('./2022_place_canvas_history.csv', chunksize=1_000_000)):

        chunk = chunk[['timestamp', 'user_id', 'pixel_color', 'coordinate']] # only relevant cols

        chunk['pixel_color'] = chunk['pixel_color'].map({
            '#FFFFFF': 'White', '#000000': 'Black', '#FF0000': 'Red', 
            '#00FF00': 'Green', '#0000FF': 'Blue', '#FFFF00': 'Yellow'
        }) # convert hex codes to plain English

        all_chunks.append(chunk)
        df = pd.concat(all_chunks, ignore_index=True)

        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_file, compression='snappy')

    return df



# compute tasks

# rank colors by distinct users function
def rank_colors_distinct_users(df):

    # groupby color (pixel_color), count num of users (user_id)
    color_rank = df.groupby('pixel_color')['user_id'].nunique().sort_values(ascending=False)
    return color_rank


# calc avg session length function
def calc_avg_session_length(df):

    # if user >1 pixel placement
    # time how long user spent 
    # take average

    # NEED TO ADD 15 min session?

    session_lengths = []

    for user_id, group in df.group_by('user_id'):
        group = group.sort_values('timestamp') # sort time for each user
        if len(group) > 1: # if user >1 pixel placement
            session_durations = group['timestamp'].diff().dt.total_seconds() # find time difference for each user
            session_lengths.extend(session_durations) # add each users duration to session lengths

    if session_lengths: # not empty, take avg
        return sum(session_lengths) / len(session_lengths)
    else:
        return 0


# pixel placement percentiles function
def calc_pixel_percentile():
    return


# count first time users
def count_first_time_users():
    return



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

    df = preprocess_data()

    df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S.%f UTC') # convert timestamp to format
    df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]

    start = time.perf_counter_ns() # start execution time

    # run tasks
    color_rank = rank_colors_distinct_users(df)
    avg_session_length = calc_avg_session_length(df)

    end = time.perf_counter_ns()
    exec_time = (end - start) / 1_000_000 # convert from ns to ms

    # print results
    print(f"Ranking of Colors by Distinct Users: {color_rank}")
    print("\nAverage Session Length: {avg_session_length:.2f} seconds")
    print(f"\nExecution Time: {exec_time:.0f} ms")


if __name__=="__main__":
    main()