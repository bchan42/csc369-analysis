# imports
import pandas as pd
from datetime import datetime
import sys
from collections import Counter
import time


# process csv function
def process_csv(start_time, end_time):

    color_count = Counter()
    pixel_count = Counter()

    # process csv in chunks
    for chunk in pd.read_csv("./2022_place_canvas_history.csv", chunksize=1_000_000):
        
        # convert timestamps to datetime
        chunk['timestamp'] = pd.to_datetime(chunk['timestamp'], format='%Y-%m-%d %H:%M:%S.%f UTC', errors='coerce')

        # filter rows within the timeframe
        chunk_timestamp = chunk[(chunk['timestamp'] >= start_time) & (chunk['timestamp'] <= end_time)]

        # increment num of colors and pixel locations
        color_count.update(chunk_timestamp['pixel_color'])
        pixel_count.update(chunk_timestamp['coordinate'])

    # find most common color and location
    most_comm_color = color_count.most_common(1)[0][0] if color_count else "No Color Data"
    most_comm_pixel = pixel_count.most_common(1)[0][0] if pixel_count else "No Pixel Location Data"

    return most_comm_color, most_comm_pixel


def main():

    try:
        start_time = datetime.strptime(sys.argv[1], "%Y-%m-%d %H") # accept time in YYYY-MM-DD HH format
        end_time = datetime.strptime(sys.argv[2], "%Y-%m-%d %H")

        if start_time >= end_time:
            print("Error: End hour must be after the start hour.")
            sys.exit(1)

    except ValueError:
        print("Error: Incorrect Date Format")
        sys.exit(1)

    # start execution time
    start = time.perf_counter_ns()

    # process data
    most_comm_color, most_comm_pixel = process_csv(start_time, end_time)

    # end execution time
    end = time.perf_counter_ns()
    exec_time = (end - start) / 1_000_000  # convert from ns to ms

    # print results
    print(f"- **Timeframe:** {start_time.strftime('%Y-%m-%d %H')} to {end_time.strftime('%Y-%m-%d %H')}")
    print(f"- **Execution Time:** {exec_time:.0f} ms")
    print(f"- **Most Placed Color:** {most_comm_color}")
    print(f"- **Most Placed Pixel Location:** {most_comm_pixel}")


if __name__ == "__main__":
    main()