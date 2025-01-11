# inputs: starting hour, ending hour
# outputs: 
    # most placed color during timeframe
    # most placed pixel location during that timeframe


# imports
import csv
from datetime import datetime
import sys
from collections import Counter

# process csv function
def process_csv(start_time, end_time):

    color_count = Counter()
    pixel_count = Counter()

    # read in csv
    with open("./2022_place_canvas_history.csv", "r") as file:
        reader = csv.reader(file, delimiter=',')

        for row in reader:
            # print(row) # print out each row of csv

            try:
                timestamp = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S.%f UTC") # convert to YYYY-MM-DD HH format
                if start_time <= timestamp <= end_time: # check start before end hour
                    id, color, location = row[1], row[2], row[3]

                    color_count[color] += 1 # increment counter and add that color/location
                    pixel_count[location] += 1

            except Exception as e:
                continue

    # find most common color & location
    most_comm_color = color_count().most_common[1] # first most common color (need to still handle case DNE)
    most_comm_pixel = pixel_count().most_common[1]


    return most_comm_color, most_comm_pixel

# example of a row
# ['2022-04-04 01:04:50.028 UTC', 
# '01Lxxk2+z9HZe+3XIfbk+lEDIBJfPhPiuMl3SYaWKKLLnjzaxft80goIPZSbFto+fWnxpJm/mS71GjYWgvrdWg==', 
# '#FFFFFF', 
# '1383,1271']

def main():

    start_time_input, end_time_input = sys.arg[0], sys.arg[1] # take in date arguments

    try:
        start_time = datetime.strptime(start_time_input, "%Y-%m-%d %H") # accept time in YYYY-MM-DD HH format
        end_time = datetime.strptime(end_time_input, "%Y-%m-%d %H")
    except:
        sys.exit(1)
    
    # return most placed color during timeframe
    # return most placed pixel location during that timeframe
    most_comm_color, most_comm_pixel = process_csv(start_time, end_time) # process data


    # save to md function for different timeframes

    print() # debugger to check

# if __name__: "__main__":
#     main()