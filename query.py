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


def main(data_path, project_id=PROJECT_ID, dataset_id=DATASET_ID,
        table_id=TABLE_NAME, num_retries=5, poll_interval=3, compression=True,
        **kwargs):

    if data_path is None:
        data_path = table_id + ".csv"
    
    bigquery = cdr.get_bigquery_service()
    query = "SELECT * FROM [{0}:{1}.{2}]".format(project_id, dataset_id, table_id)
    if kwargs != {}:
        query += " WHERE " + "\n\tAND ".join("{0}={1!r}".format(k, v) for k, v in kwargs.items())
    # Qyery Job data:
    job_id = str(uuid.uuid4()) 
    query_data=dict(
            query=query,
            jobReference={
                'projectId': project_id,
                'job_id': job_id
            })
    job = bigquery.jobs()
    res = job.query(
            projectId=project_id,
            body=query_data).execute()

    #return res
    if compression and not data_path.endswith(".gz"):
        data_path += ".gz"

    with gzip.GzipFile(data_path, "wb") if compression else open(data_path, "wb") as df:
        csv_out = csv.writer(df)
        csv_out.writerow([c["name"] for c in res["schema"]["fields"]])
        csv_out.writerows(([e['v'] for e in row['f']] for row in res["rows"]))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        'data_path',
        help='Path to the CSV data (default: derived from the table name).',
        nargs='?', default=None)
    parser.add_argument("-P", '--project_id', help='Your Google Cloud project ID (default: {0}).'.format(PROJECT_ID), default=PROJECT_ID)
    parser.add_argument("-d", '--dataset_id', help='A BigQuery dataset ID (default: {0}).'.format(DATASET_ID), default=DATASET_ID)
    parser.add_argument(
            "-t", '--table_id', help='Name of the table to query (defeult: {0}).'.format(TABLE_NAME), default=TABLE_NAME)
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
                help='Query field "{0}" ({1}).'.format(name, field_type),
                type=int if field_type == cdr.INTEGER else float if field_type == cdr.FLOAT else str)

    args = parser.parse_args()
    # extract query properties:
    properties = {
            k: v for k, v in vars(args).items()
            if v is not None and k in cdr.schema_dict.keys()}

    main(
        args.data_path,
        args.project_id,
        args.dataset_id,
        args.table_id,
        args.num_retries,
        args.poll_interval,
        compression="GZIP" if args.gzip else "NONE",
        **properties)
