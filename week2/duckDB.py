# imports
import duckdb
from datetime import datetime
import sys
import time


# process csv function
def process_csv(start_time, end_time):

    # connect to duckDB
    con = duckdb.connect()

    # query to read in csv and filter by timestamp range
    query = f"""
        SELECT pixel_color, coordinate
        FROM read_csv_auto('./2022_place_canvas_history.csv', header=True)
        WHERE timestamp >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}' AND timestamp <= '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'
    """
    
    # execute query and load data into results
    result = con.execute(query).fetchall()

    # close connection
    con.close()

    # initialize dictionary for color & location
    color_count = {} 
    pixel_count = {}

    for row in result:
        color, location = row
        color_count[color] = color_count.get(color, 0) + 1 # increment num of colors and pixel locations
        pixel_count[location] = pixel_count.get(location, 0) + 1

    # find most common color and pixel location
    most_comm_color = max(color_count, key=color_count.get, default="No Color Data")
    most_comm_pixel = max(pixel_count, key=pixel_count.get, default="No Pixel Location Data")

    return most_comm_color, most_comm_pixel


def main():

    try:
        start_time = datetime.strptime(sys.argv[1], "%Y-%m-%d %H")  # accept time in YYYY-MM-DD HH format
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