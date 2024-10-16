from configparser import ConfigParser
from connect import connect
import entity
from fetch import read_files
import os
from dotenv import load_dotenv, dotenv_values

import getopt, sys

def main():
    load_dotenv()

    connection = connect()
    argumentList = sys.argv[1:]
    path = parse_argument(argumentList)
    read_files(connection, path)

def parse_argument(args):
    options = "f:"
    long_options = ["file"]

    try:
        # Parsing argument
        arguments, values = getopt.getopt(args, options, long_options)

        # checking each argument
        for currentArgument, currentValue in arguments:
            if currentArgument in ("-f", "--file"):
                return currentValue

    except getopt.error as err:
        # output error, and return with an error code
        print(str(err))

main()

