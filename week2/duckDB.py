# imports
import duckDB
from datetime import datetime
import sys
import time


# process csv function
def process_csv_duckdb(start_time, end_time):

    # connect to duckDB
    conn = duckDB.connect()

    # load in csv
    conn.execute(f"""
        CREATE TABLE data AS 
        SELECT * FROM read_csv_auto('./2022_place_canvas_history.csv');
    """)

    # convert timestamps to duckDB
    start_str = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_time.strftime("%Y-%m-%d %H:%M:%S")

    # query for most common color & pixel loc
    query = f"""


    """

    # execute query
    result = conn.execute(query).fetchall()

    # close connection
    conn.close()

    # validation on whether color/pixel exists
    if result:
        most_comm_color, most_comm_pixel = result[0]
    else:
        most_comm_color, most_comm_pixel = "no color data", "no pixel loc data"
 
    return most_comm_color, most_comm_pixel


# main
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

    # start execution time
    start = time.perf_count_ns()

    # return most placed color during timeframe
    # return most placed pixel location during that timeframe
    most_comm_color, most_comm_pixel = process_csv_duckdb(start_time, end_time)

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