#!/usr/bin/python
# --------------------------------------------------------
#
# PYTHON PROGRAM DEFINITION
#
# The knowledge a computer has of Python can be specified in 3 levels:
# (1) Prelude knowledge --> The computer has it by default.
# (2) Borrowed knowledge --> The computer gets this knowledge from 3rd party libraries defined by others
#                            (but imported by us in this program).
# (3) Generated knowledge --> The computer gets this knowledge from the new functions defined by us in this program.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer first processes this PYTHON PROGRAM DEFINITION section of the file.
# On it, our computer enhances its Python knowledge from levels (2) and (3) with the imports and new functions
# defined in the program. However, it still does not execute anything.
#
# --------------------------------------------------------

# ------------------------------------------
# IMPORTS
# ------------------------------------------
import sys
import codecs
from itertools import islice


def divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]
    return l


def process_line(line):
    # 1. We create the output variable
    res = ()

    universal = None
    # 1.1. We output the letter from the line
    bike_id = None



    # 1.2. We ouptut the num_words from the line
    duration_time = None

    # 1.3. We ouptut the total_length_words from the line
    number_trips = None

    list_of_start_times = ""
    list_of_stop_times = ""
    list_of_start_stations = ""
    list_of_stop_stations = ""

    # 2. We assign the variables
    content = line.strip().split("\t")
    universal = content[0]

    aux_content = content[1].split(" @ ")
    chunky = list(divide_chunks(aux_content, 4))
    #print(chunky)
    chunked = [aux_content[i:i + 4] for i in range(0, len(aux_content), 4)]
    #aux_content = [aux_content[4*i:4*(i+1)] for i in range(len(aux_content)/4 + 1)]
    for i, x in enumerate(aux_content):

        if i % 4 == 0:
            x = x.replace("(", "")
            list_of_start_times = x
        if i % 4 == 1:
            list_of_stop_times = x
        if i % 4 == 2:
            list_of_start_stations = x
        if i % 4 == 3:
            x = x.replace(")", "")
            list_of_stop_stations = x

    res = (universal, list_of_start_times, list_of_stop_times, list_of_start_stations, list_of_stop_stations)
    print(res)
    # 4. We return res
    return res


def write_line(my_output_stream, previous_stop_t, previous_stop_s, current_start_s, current_start_t):
    # 1. We create the String
    my_str = "By_Truck\t" + "(" + str(previous_stop_t) + ", " + str(previous_stop_s) + ", " \
             + str(current_start_s) + ", " + str(current_start_t) + ")" + "\n"

    # 2. We write it to the file
    my_output_stream.write(my_str)
# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters):
    pass
    first = True
    current_start_time = ""
    current_start_station = ""
    for line in my_input_stream:
        (universe, start_t, stop_t, start_s, stop_s) = process_line(line)
        if first:
            first = False
        else:
            if start_s != current_start_station:
                write_line(my_output_stream, stop_t, stop_s, current_start_station, current_start_time)
        current_start_time = stop_t
        current_start_station = stop_s
        #print(start_s)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We collect the input values
    file_name = "sort_1.txt"

    # 1.1. If we call the program from the console then we collect the arguments from it
    if (len(sys.argv) > 1):
        file_name = sys.argv[1]

    # 2. Local or Hadoop
    local_False_hadoop_True = False

    # 3. We set the path to my_dataset and my_result
    my_input_stream = sys.stdin
    my_output_stream = sys.stdout

    if (local_False_hadoop_True == False):
        my_input_stream = "../../my_results/A01_Part6/2_my_sort_simulation/" + file_name
        my_output_stream = "../../my_results/A01_Part6/3_my_reduce_simulation/reduce_" + file_name[5:]

    # 4. my_reducer.py input parameters
    my_reducer_input_parameters = []

    # 5. We call to my_main
    my_map(my_input_stream, my_output_stream, my_reducer_input_parameters)
