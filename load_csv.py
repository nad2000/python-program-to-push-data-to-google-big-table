#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: ai ts=4 sts=4 et sw=4 ft=python

import argparse
import json
import time
import uuid

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from apiclient.http import MediaFileUpload

# Default values:
PROJECT_ID = "cdrstore-1216"
DATASET_ID = "cdrstore"
TABLE_NAME = "cdr"

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
                    'destinationTable': {
                        'projectId': project_id,
                        'datasetId': dataset_id,
                        'tableId': table_name
                    },
                    'sourceFormat': source_format,
                    "fieldDelimiter": '?'
                }
            }
        },
        media_body=MediaFileUpload(
            data_path,
            mimetype="application/x-gzip" if data_path.split('.')[-1] == "gz" else 'application/octet-stream'))
    job = insert_request.execute(num_retries=num_retries)

    return job


def poll_job(bigquery, job):
    """Waits for a job to complete."""

    print('Waiting for job to finish...')

    request = bigquery.jobs().get(
        projectId=job['jobReference']['projectId'],
        jobId=job['jobReference']['jobId'])

    while True:
        result = request.execute(num_retries=2)

        if result['status']['state'] == 'DONE':
            if 'errorResult' in result['status']:
                raise RuntimeError(result['status']['errorResult'])
            print('Job complete.')
            return

        time.sleep(1)


def main(project_id, dataset_id, table_name, data_path,
         poll_interval, num_retries):
    # Grab the application's default credentials from the environment.
    credentials = GoogleCredentials.get_application_default()

    # Construct the service object for interacting with the BigQuery API.
    bigquery = discovery.build('bigquery', 'v2', credentials=credentials)

    job = load_table(
        bigquery,
        data_path,
        project_id,
        dataset_id,
        table_name,
        num_retries)

    poll_job(bigquery, job)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        'data_path',
        help='Google Cloud Storage path to the CSV data, for example: '
             'gs://mybucket/in.csv')
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
        args.project_id,
        args.dataset_id,
        args.table_name,
        args.data_path,
        args.poll_interval,
        args.num_retries)
