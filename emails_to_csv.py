'''
This script parses email files and extracts relevant features to a CSV file.
'''

import argparse
import pandas as pd
import os
import docx2txt
import tempfile
import unicodedata
import time
from datetime import datetime
from pandas import DataFrame
from argparse import Namespace
from argparse import ArgumentParser
from logging import Logger

DATETIME_FORMAT = "%m/%d/%Y %I:%M:%S %p"

def get_args_parser() -> ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='Emails-to-CSV Script',
        description='Extracts relevant features from email files and saves them to a CSV file.',
        add_help=True,
    )

    parser.add_argument(
        'emails_dir',
        type=str,
        help='Path to directory containing email files',
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        default='',
        help='Path to directory to save extracted features to'
    )

    parser.add_argument(
        '--chunk-size',
        type=int,
        default=1000,
        help='Write to CSV every chunk size number of emails processed. If 0 or less, write to CSV will only occur once all emails have been processed.',
    )

    parser.add_argument(
        '--log-to-file',
        action='store_true',
        help='If specified, write log statements to a file'
    )

    return parser

def parse_email(email_pth:str, remove_pth:bool=False) -> DataFrame:
    features = {
        'dirname': [''], # parent directory name containing email
        'fname': [''], # file name of email
        'from': [''], # sender's email address
        'sent_time': [None], # date+time email was sent
        'to': [''], # who email was sent to
        'cc': [''], # any email addresses cc'd
        'bcc': [''], # any email addresses bcc'd
        'subject': [''], # subject line of email
        'body': [''], # email body contents
        }

    features['dirname'] = [os.path.dirname(email_pth).split('/')[-1]]
    features['fname'] = [os.path.basename(email_pth)]
    body_reached = False
    body_contents = []

    with open(email_pth, mode='r') as f:
        from_line = None
        sent_time_line = None
        to_line = None
        cc_line = None
        bcc_line = None
        subject_line = None

        for i, line in enumerate(f):
            line = unicodedata.normalize('NFKC', line.strip())

            if body_reached:
                body_contents.append(line)
                continue

            if from_line is None and line.startswith('From:'):
                from_line = i+1
                continue
            elif sent_time_line is None and line.startswith('Sent time:'):
                sent_time_line = i+1
                continue
            elif to_line is None and line.startswith('To:'):
                to_line = i+1
                continue
            elif cc_line is None and line.startswith('Cc:'):
                cc_line = i+1
                continue
            elif bcc_line is None and line.startswith('BCc:'):
                bcc_line = i+1
                continue
            elif subject_line is None and line.startswith('Subject:'):
                subject_line = i+1
                continue

            if i == from_line:
                features['from'] = line
            elif i == sent_time_line:
                features['sent_time'] = line
            elif i == to_line:
                features['to'] = line
            elif i == cc_line:
                features['cc'] = line
            elif i == bcc_line:
                features['bcc'] = line
            elif i == subject_line:
                features['subject'] = line
            else:
                body_contents.append(line)
                body_reached = True

    if remove_pth:
        os.remove(email_pth)

    features['body'] = [' '.join(body_contents)]

    return DataFrame.from_dict(features)

def docx_to_tmp_txt(docx_pth: str):
    txt = docx2txt.process(docx_pth)

    # Normalize text: replace multiple newlines with a single newline
    normalized_txt = '\n'.join(line.strip() for line in txt.splitlines() if line.strip())

    with tempfile.NamedTemporaryFile(mode='w', prefix=os.path.basename(docx_pth), suffix='.txt', delete=False) as tmp_file:
        tmp_file.write(normalized_txt)
        tmp_pth = tmp_file.name

    return tmp_pth

def extract_features(emails_dir:str, chunk_size:int|None=None, output_dir:str='', logger:Logger|None=None):
    feature_chunks = []
    skipped_emails = []

    dirname = os.path.basename(emails_dir)
    output_fname = f'{dirname}_emails_{datetime.now().strftime("%m%d%y-%H%M%S")}.csv'
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    output_pth = os.path.join(args.output_dir, output_fname)
    
    start_time = time.time()

    with os.scandir(emails_dir) as fiter:
        chunk_i = 0
        chunk_start = time.time()

        for i, f in enumerate(fiter):            
            is_txt = f.name.endswith('.txt')
            is_docx = f.name.endswith('.docx')

            if f.is_file() and (is_txt or is_docx):
                if logger is not None: logger.info(f'Parsing email {f.name} ({i})')
                
                email_pth = os.path.join(emails_dir, f.name)
                try:
                    if is_txt:
                        email_features = parse_email(email_pth)
                    elif is_docx:
                        tmp_txt_pth = docx_to_tmp_txt(email_pth)
                        email_features = parse_email(tmp_txt_pth, remove_pth=True)
                        email_features['dirname'] = dirname
                        email_features['fname'] = f.name
                except Exception as e:
                    logger.error(f'Error processing email {email_pth}: {e} ... Skipping file')
                    skipped_emails.append(email_pth)
                    continue

                feature_chunks.append(email_features)

                try:
                    if chunk_size is not None and len(feature_chunks) >= chunk_size:
                        tmp_df = pd.concat(feature_chunks, axis=0)
                        tmp_df.to_csv(
                            output_pth, 
                            mode='a', 
                            header=not os.path.exists(output_pth), 
                            index=False,
                            ) # see https://stackoverflow.com/a/17975690/
                        
                        chunk_total = time.time() - chunk_start
                        if logger is not None: logger.info(f'Wrote chunk {chunk_i} to {output_pth} in {chunk_total} seconds')

                        chunk_start = time.time()
                        feature_chunks = []
                        chunk_i += 1
                except Exception as e:
                    if logger is not None: logger.critical(f'Error writing chunk {chunk_i}: {e} ... Terminating')
                    raise e

    if feature_chunks:
        tmp_df = pd.concat(feature_chunks, axis=0)
        tmp_df.to_csv(
            output_pth, 
            mode='a', 
            header=not os.path.exists(output_pth), 
            index=False,
            )

    end_time = time.time()
    if logger is not None: 
        logger.warning(f'Skipped {len(skipped_emails)} email(s) during processing: {skipped_emails}')
        logger.info(f'Total processing time: {end_time-start_time} seconds')
        
def main(args:Namespace):
    import logging
    import sys
    from logging import FileHandler, StreamHandler

    emails_dir = args.emails_dir

    chunk_size = args.chunk_size if args.chunk_size >= 1 else None

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
            filename=f'{os.path.basename(emails_dir)}_emails_conversion.log'
        )
        file_handler.setFormatter(log_formatter)
        logger.addHandler(file_handler)

    logger.info(f'Began processing of emails in {emails_dir}')
    
    extract_features(emails_dir, chunk_size=chunk_size, output_dir=args.output_dir, logger=logger)

    logger.info(f'Done processing emails in {emails_dir}')

if __name__ == '__main__':
    parser = get_args_parser()
    args = parser.parse_args()
    main(args)