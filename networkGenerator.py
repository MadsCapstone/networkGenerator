import sys
import DataHandler
import CreateNetworkFromDF
import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--data', type=str, nargs='*', help='data files to read into preparation')
    parser.add_argument('--config', nargs='*')
    # parser.add_argument('-', dest='accumulate', action='store_const',
    #                     const=sum, default=max,
    #                     help='sum the integers (default: find the max)')

