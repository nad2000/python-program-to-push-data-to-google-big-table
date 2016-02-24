#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: ai ts=4 sts=4 et sw=4 ft=python

from __future__ import print_function

import argparse
import uuid
from tempfile import TemporaryFile
import gzip
import csv
import time
import operator

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from apiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError

import cdr
from cdr import PROJECT_ID, DATASET_ID, TABLE_NAME


def load_table(
        bigquery, data_path, project_id=PROJECT_ID,
        dataset_id=DATASET_ID, table_name=TABLE_NAME, num_retries=5):
    source_format = 'CSV'
    if data_path[-5:].lower() == '.json':
        source_format = 'NEWLINE_DELIMITED_JSON'

    insert_request = bigquery.jobs().insert(
        projectId=project_id,
        body={
            'jobReference': {
                'projectId': project_id,
                'job_id': str(uuid.uuid4())
            },
            'configuration': {
                'load': {
                    "schema": {"fields": cdr.schema_fields},
                    'destinationTable': {
                        'projectId': project_id,
                        'datasetId': dataset_id,
                        'tableId': table_name
                    },
                    'sourceFormat': source_format,
                    "fieldDelimiter": ','
                }
            }
        },
        media_body=MediaIoBaseUpload(
            fd=cdr.convert_csv(data_path),
            mimetype="application/x-gzip" if data_path.split('.')[-1] == "gz" else 'application/octet-stream')
    )
    job = insert_request.execute(num_retries=num_retries)

    return job


def main(project_id, dataset_id, table_name, data_path,
         poll_interval, num_retries):
    print(project_id, dataset_id, table_name, data_path,
         poll_interval, num_retries)
    
    bigquery = cdr.get_bigquery_service()
    
    while True:
        try:
            job = load_table(
                bigquery,
                data_path,
                project_id,
                dataset_id,
                table_name,
                num_retries)

            cdr.poll_job(bigquery, job)
            break  ## SUCCESS

        except HttpError as e:
            if "not found: dataset" in e.content.lower():
                # create a data set and try again:
                cdr.create_dataset(bigquery, project_id, dataset_id)
            
        except Exception as e:
            raise Exception, "Unhandled exception occured", e

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

    args = parser.parse_args()

    main(
        project_id=args.project_id,
        dataset_id=args.dataset_id,
        table_name=args.table_name,
        data_path=args.data_path,
        poll_interval=args.poll_interval,
        num_retries=args.num_retries)
