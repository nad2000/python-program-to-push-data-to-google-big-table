#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: ai ts=4 sts=4 et sw=4 ft=python

import argparse
import uuid
from tempfile import TemporaryFile
import gzip
import csv
import time
import operator

from apiclient.http import MediaIoBaseUpload

import cdr
from cdr import PROJECT_ID, DATASET_ID, TABLE_NAME



def main(project_id, dataset_id, table_name, data_path,
         poll_interval, num_retries):
    
    bigquery = cdr.get_bigquery_service()

    cdr.poll_job(bigquery, job)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        'data_path',
        help='Path to the CSV data, for example: ')
    parser.add_argument("-P", '--project_id', help='Your Google Cloud project ID (default: {0}).'.format(PROJECT_ID), default=PROJECT_ID)
    parser.add_argument("-d", '--dataset_id', help='A BigQuery dataset ID (default: {0}).'.format(DATASET_ID), default=DATASET_ID)
    parser.add_argument(
            "-t", '--table_name', help='Name of the table to load data into (defeult: {0}).'.format(TABLE_NAME), default=TABLE_NAME)
    parser.add_argument(
        '-p', '--poll_interval',
        help='How often to poll the query for completion (seconds).',
        type=int,
        default=1)
    parser.add_argument(
        '-r', '--num_retries',
        help='Number of times to retry in case of 500 error.',
        type=int,
        default=5)
    parser.add_argument(
        '-z', '--gzip',
        help='compress resultset with gzip',
        action='store_true',
        default=False)

    for name, field_type, nullable, skip in cdr.fields:
        if not skip and not name.startswith("DUMMY"):
            parser.add_argument(
                '--' + name,
                help='Query field "{}".'.format(name),
                type=int if field_type == cdr.INCLUDE else float if field_type == cdr.FLOAT else str)

    args = parser.parse_args()

#    main(
#        args.gcs_path,
#        args.project_id,
#        args.dataset_id,
#        args.table_id,
#        args.num_retries,
#        args.poll_interval,
#        compression="GZIP" if args.gzip else "NONE")
