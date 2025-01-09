# inputs: starting hour, ending hour
# outputs: 
    # most placed color during timeframe
    # most placed pixel location during that timeframe


# read in csv
import csv

with open("./2022_place_canvas_history.csv", "r") as file:
    reader = csv.reader(file, delimiter=',')
    for row in reader:
        # print(row) # print out each row of csv
        timestamp_str, id, color, location = row

# example of a row
# ['2022-04-04 01:04:50.028 UTC', 
# '01Lxxk2+z9HZe+3XIfbk+lEDIBJfPhPiuMl3SYaWKKLLnjzaxft80goIPZSbFto+fWnxpJm/mS71GjYWgvrdWg==', 
# '#FFFFFF', 
# '1383,1271']

# accept starting and ending hours from command line
# check format: YYYY-MM-DD HH
from datetime import datetime
import sys

def check_format(date_str):
    try: 
        return datetime.strftime(date_str, "%Y-%m-%d %H")
    except ValueError:
        sys.exit(1)

# start hour before end hour
start_time = check_format(sys.argv[1])
end_time = check_format(sys.argv[2])

if start_time >= end_time:
    sys.exit(1)


# return most placed color during timeframe
# return most placed pixel location during that timeframe