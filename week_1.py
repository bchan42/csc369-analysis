# inputs: starting hour, ending hour
# outputs: 
    # most placed color during timeframe
    # most placed pixel location during that timeframe


# read in csv
import csv

with open("./2022_place_canvas_history.csv", "r") as file:
    reader = csv.reader(file, delimiter=',')
    for row in reader:
        print(row) # print out each row of csv


# example of a row
# ['2022-04-04 01:04:50.028 UTC', 
# '01Lxxk2+z9HZe+3XIfbk+lEDIBJfPhPiuMl3SYaWKKLLnjzaxft80goIPZSbFto+fWnxpJm/mS71GjYWgvrdWg==', 
# '#FFFFFF', 
# '1383,1271']

# accept starting and ending hours from command line
# check format: YYYY-MM-DD HH
# start hour before end hour
# ...


# return most placed color during timeframe
# return most placed pixel location during that timeframe