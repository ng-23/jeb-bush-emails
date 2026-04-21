'''
This script reads all CSVs in a directory and stitches them together into 1 big CSV.
'''
import argparse
import pandas as pd
import os
import time
import sys
from pandas import DataFrame
from argparse import Namespace
from argparse import ArgumentParser
from datetime import datetime
from logging import Logger

SCRIPT_TIME = datetime.now()

def get_args_parser() -> ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='CSV Stictching Script',
        description='Combines multiple CSV files together into 1.',
        add_help=True,
    )

    parser.add_argument(
        'csvs_dir',
        type=str,
        help='Path to directory containing CSV files.',
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        default='',
        help='Path to directory to combined CSV to.'
    )

    parser.add_argument(
        '--output-fname',
        type=str,
        default=f'all_emails_{SCRIPT_TIME.strftime("%m%d%y-%H%M%S")}.csv',
        help='Name of output CSV file.'
    )

    parser.add_argument(
        '--rchunk-size',
        type=int,
        default=1000,
        help='Read chunk size number of rows from CSVs at a time. If 0 or less, all rows will be fully read into RAM.',
    )

    parser.add_argument(
        '--wchunk-size',
        type=int,
        default=1000,
        help='Write to CSV every chunk size number of emails processed. If 0 or less, write to CSV will only occur once all CSVs have been processed.',
    )

    parser.add_argument(
        '--log-to-file',
        action='store_true',
        help='If specified, write log statements to a file.'
    )

    return parser

def stitch(
        csvs_dir:str, 
        rchunk_size:int, 
        wchunk_size:int, 
        output_fname:str, 
        output_dir:str='', 
        logger:Logger|None=None):
    
    output_pth = os.path.join(output_dir, output_fname)
    rchunk_size = sys.maxsize if rchunk_size <= 0 else rchunk_size
    wchunk_size = sys.maxsize if wchunk_size <= 0 else wchunk_size
    wchunks = []

    if logger is not None: logger.info(f'Stitching w/ read chunk size of {rchunk_size} and write chunk size of {wchunk_size} to {output_pth}')

    start_time = time.time()

    with os.scandir(csvs_dir) as fiter:
        chunk_i = 0
        for i, f in enumerate(fiter):
            if f.is_file() and f.name.endswith('.csv'):
                if logger is not None: logger.info(f'Stitching CSV {f.path} ({i})')

                try:
                    rchunks = pd.read_csv(f.path, chunksize=rchunk_size)
                except Exception as e:
                    if logger is not None: logger.critical(f'Error reading CSV {f.path}: {e} ... Terminating')
                    raise e
                
                for j, rchunk in enumerate(rchunks):
                    wchunks.append(rchunk)
                    if logger is not None: logger.info(f'Read chunk {j} of {f.path}')

                    if len(wchunks) >= wchunk_size:
                        try:
                            temp_df = pd.concat(wchunks, axis=0)
                            temp_df.to_csv(
                                output_pth, 
                                mode='a',
                                header=not os.path.exists(output_pth), 
                                index=False,
                            )
                            wchunks = []
                            if logger is not None: logger.info(f'Wrote chunk {chunk_i} to {output_pth}')
                            chunk_i += 1
                        except Exception as e:
                            if logger is not None: logger.critical(f'Error writing chunk {chunk_i} to {output_pth}: {e} ... Terminating')
                            raise e
    if wchunks:
        tmp_df = pd.concat(wchunks, axis=0)
        tmp_df.to_csv(
            output_pth, 
            mode='a', 
            header=not os.path.exists(output_pth), 
            index=False,
            )
        
    end_time = time.time()
    if logger is not None: 
        logger.info(f'Total stitching time: {end_time-start_time} seconds')

def main(args:Namespace):
    import logging
    import sys
    from logging import FileHandler, StreamHandler

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    
    log_formatter = logging.Formatter(
        fmt='%(levelname)s @ %(asctime)s: %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S %p',
        )
    
    console_handler = StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)

    if args.log_to_file:
        file_handler = FileHandler(
            filename=f'csv_stitch_{SCRIPT_TIME.strftime("%m%d%y-%H%M%S")}.log'
        )
        file_handler.setFormatter(log_formatter)
        logger.addHandler(file_handler)

    logger.info(f'Began vertical stitching of CSVs in {args.csvs_dir}')

    stitch(
        csvs_dir=args.csvs_dir, 
        rchunk_size=args.rchunk_size, 
        wchunk_size=args.wchunk_size, 
        output_fname=args.output_fname, 
        output_dir=args.output_dir, 
        logger=logger,
        )
    
    logger.info(f'Finished vertical stitching of CSVs in {args.csvs_dir}')

if __name__ == '__main__':
    parser = get_args_parser()
    args = parser.parse_args()
    main(args)