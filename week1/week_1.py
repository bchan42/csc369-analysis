# inputs: starting hour, ending hour
# outputs: 
    # most placed color during timeframe
    # most placed pixel location during that timeframe


# imports
import csv
from datetime import datetime
import sys
from collections import Counter
import time

# process csv function
def process_csv(start_time, end_time):

    color_count = Counter()
    pixel_count = Counter()

    # read in csv
    with open("./2022_place_canvas_history.csv", "r") as file:
        reader = csv.reader(file, delimiter=',')

        next(reader, None) # skip header

        for row in reader:
            # print(row) # print out each row of csv
            try:
                timestamp = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f UTC") # convert to datetime object for comparison
                if start_time <= timestamp <= end_time: # check start before end hour
                    id, color, location = row[1], row[2], row[3]

                    color_count[color] += 1 # increment counter and add that color/location
                    pixel_count[location] += 1

            except ValueError:
                print(f'Skip row: {row}')
                continue

    # find most common color & location
    # if there is a tie, most_common returns the first one it encounters
    most_comm_color = color_count.most_common(1)[0][0] if color_count else "No Color Data" # first most common color & handles case DNE)
    most_comm_pixel = pixel_count.most_common(1)[0][0] if pixel_count else "No Pixel Location Data"


    return most_comm_color, most_comm_pixel

# example of a row
# ['2022-04-04 01:04:50.028 UTC', 
# '01Lxxk2+z9HZe+3XIfbk+lEDIBJfPhPiuMl3SYaWKKLLnjzaxft80goIPZSbFto+fWnxpJm/mS71GjYWgvrdWg==', 
# '#FFFFFF', 
# '1383,1271']

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
    most_comm_color, most_comm_pixel = process_csv(start_time, end_time) # process data

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