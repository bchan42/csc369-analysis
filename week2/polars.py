# imports
import polars as pl
from datetime import datetime
import sys
import time

# process csv function
def process_csv_pl(start_time, end_time):

    # read in csv
    df = pl.read_csv("./2022_place_canvas_history.csv")

    # convert timestamp to datetime
    df = df.with_columns(pl.col('timestamp').str.strptime(pl.Datetime, 
                                                          fmt="%Y-%m-%d %H:%M:%S.%f UTC"))
    
    # filter df with given timeframe
    df_timeframe = df.filter((pl.col('timestamp') >= start_time) & (pl.col('timestamp') <= end_time))

    # results (try something else) -- series mode?
    most_comm_color = df_timeframe['color'].mode()[0] if len(df_timeframe['color'].mode()) > 0 else "No Color Data"
    most_comm_pixel = df_timeframe['location'].mode()[0] if len(df_timeframe['location'].mode()) > 0 else "No Pixel Location Data"

    return most_comm_color, most_comm_pixel


def main():

    try:
        start_time = datetime.strptime(sys.argv[1], "%Y-%m-%d %H") # accept time in YYYY-MM-DD HH format
        end_time = datetime.strptime(sys.argv[2], "%Y-%m-%d %H")

        if start_time >= end_time:
            print("Error: End hour must be after the start hour.")

    except ValueError:
        print("Error: Incorrect Date Format")
        sys.exit(1)

    # start execution time
    start = time.perf_counter_ns()
    
    # return most placed color during timeframe
    # return most placed pixel location during that timeframe
    most_comm_color, most_comm_pixel = process_csv_pl(start_time, end_time) # process data

    # end execution time
    end = time.perf_counter_ns()
    exec_time = (end - start) / 1_000_000 # convert from ns to ms

    # print result
    print(f"- **Timeframe:** {start_time.strftime('%Y-%m-%d %H')} to {end_time.strftime('%Y-%m-%d %H')}")
    print(f"- **Execution Time:** {exec_time:.0f} ms")
    print(f"- **Most Placed Color:** {most_comm_color}")
    print(f"- **Most Placed Pixel Location:** {most_comm_pixel}")


if __name__ == "__main__":
    main()