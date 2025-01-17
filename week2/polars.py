# imports
import polars as pl
from datetime import datetime
import sys
from collections import Counter
import time


# process csv function
def process_csv(start_time, end_time):

    color_count = Counter()
    pixel_count = Counter()

    batch_size = 100_000  # num rows to process per batch

    # use polars' lazy frame (reduces memory usage)
    # scan_csv filters row while reading instead of loading in entire dataset 
    lazy_frame = pl.scan_csv("./2022_place_canvas_history.csv")

    # iterate over slices after ..
    for df_chunk in lazy_frame.with_columns(
        pl.col("timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f UTC", strict=False)
    ).filter( #  filtering specific timestamp
        (pl.col("timestamp") >= start_time) & (pl.col("timestamp") <= end_time) # starttime <= timestamp <= endtime
    ).collect().iter_slices(batch_size): # data in batches reduces memory storage too

        # increment num of colors & pixel locs
        color_count.update(df_chunk["pixel_color"].to_list())
        pixel_count.update(df_chunk["coordinate"].to_list())

    # find most common color and location
    most_comm_color = color_count.most_common(1)[0][0] if color_count else "No Color Data"
    most_comm_pixel = pixel_count.most_common(1)[0][0] if pixel_count else "No Pixel Location Data"
    
    return most_comm_color, most_comm_pixel


def main():

    try:
        start_time = datetime.strptime(sys.argv[1], "%Y-%m-%d %H")
        end_time = datetime.strptime(sys.argv[2], "%Y-%m-%d %H")

        if start_time >= end_time:
            print("Error: End hour must be after the start hour.")
            sys.exit(1)

    except ValueError:
        print("Error: Incorrect Date Format")
        sys.exit(1)

    start = time.perf_counter_ns() # start execution time
    most_comm_color, most_comm_pixel = process_csv(start_time, end_time) # process data
    end = time.perf_counter_ns() # end execution time
    exec_time = (end - start) / 1_000_000 # convert ns to ms

    # print results
    print(f"- **Timeframe:** {start_time.strftime('%Y-%m-%d %H')} to {end_time.strftime('%Y-%m-%d %H')}")
    print(f"- **Execution Time:** {exec_time:.0f} ms")
    print(f"- **Most Placed Color:** {most_comm_color}")
    print(f"- **Most Placed Pixel Location:** {most_comm_pixel}")


if __name__ == "__main__":
    main()