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

def process_line(line):
    # 1. We create the output variable
    res = ()

    # 1.1. We output the letter from the line
    station_name = None

    # 1.2. We ouptut the num_words from the line
    num_beginning_station = None

    # 1.3. We ouptut the total_length_words from the line
    num_end_station = None

    # 2. We assign the variables
    content = line.strip().split("\t")
    station_name = content[0]

    aux_content = content[1].split(", ")
    num_beginning_station = int(aux_content[0][1:])
    num_end_station = int(aux_content[1][:-1])

    # 3. We assign res
    res = (station_name, num_beginning_station, num_end_station)

    # 4. We return res
    return res

def write_line(my_output_stream, station, begin_num, end_num):
    # 1. We create the String
    my_str = station + "\t" + "(" + str(begin_num) + ", "+str(end_num)+ ")" + "\n"

    # 2. We write it to the file
    my_output_stream.write(my_str)

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters):
    pass

    current_station_name = ""
    current_beginning_station_count = 0
    current_ending_station_count = 0

    for line in my_input_stream:
        (station_name, beginning_station_count,
         ending_station_count) = process_line(line)

        if station_name != current_station_name:
            if current_station_name != "":
                write_line(my_output_stream,current_station_name, current_beginning_station_count, current_ending_station_count)
            current_station_name = station_name
            current_beginning_station_count = 0
            current_ending_station_count = 0

        current_beginning_station_count += beginning_station_count
        current_ending_station_count += ending_station_count
    if current_station_name != "":
        write_line(my_output_stream,current_station_name, current_beginning_station_count, current_ending_station_count)






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
        my_input_stream = "../../my_results/A01_Part4/2_my_sort_simulation/" + file_name
        my_output_stream = "../../my_results/A01_Part4/3_my_reduce_simulation/reduce_" + file_name[5:]

    # 4. my_reducer.py input parameters
    my_reducer_input_parameters = []

    # 5. We call to my_main
    my_reduce(my_input_stream, my_output_stream, my_reducer_input_parameters)
