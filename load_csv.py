#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: ai ts=4 sts=4 et sw=4 ft=python

import argparse
import json
import time
import uuid
from tempfile import TemporaryFile
import gzip
import csv
import operator

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from apiclient.http import MediaFileUpload, MediaIoBaseUpload

# Default values:
PROJECT_ID = "cdrstore-1216"
DATASET_ID = "cdrstore"
TABLE_NAME = "cdr"

SKIP = True
INCLUDE = False
INTEGER = "INTEGER"
STRING = "STRING"
FLOAT = "FLOAT"
NULLABLE = "NULLABLE"

fields = [
    ("connection_type", STRING, NULLABLE, SKIP),
    ("session_id", STRING, NULLABLE, SKIP),
    ("release_cause", INTEGER, NULLABLE, INCLUDE),
    ("start_time_of_date", INTEGER, NULLABLE, INCLUDE),
    ("answer_time_of_date", INTEGER, NULLABLE, INCLUDE),
    ("release_tod", INTEGER, NULLABLE, SKIP),
    ("minutes_west_of_greenwich_mean_time", INTEGER, NULLABLE, SKIP),
    ("release_cause_from_protocol_stack", STRING, NULLABLE, INCLUDE),
    ("binary_value_of_release_cause_from_protocol_stack", STRING, NULLABLE, INCLUDE),
    ("first_release_dialogue", STRING, NULLABLE, SKIP),
    ("trunk_id_origination", STRING, NULLABLE, INCLUDE),
    ("voip_protocol_origination", STRING, NULLABLE, SKIP),
    ("origination_source_number", INTEGER, NULLABLE, INCLUDE),
    ("origination_source_host_name", STRING, NULLABLE, INCLUDE),
    ("origination_destination_number", INTEGER, NULLABLE, INCLUDE),
    ("origination_destination_host_name", STRING, NULLABLE, INCLUDE),
    ("origination_call_id", STRING, NULLABLE, INCLUDE),
    ("origination_remote_payload_ip_address", STRING, NULLABLE, SKIP),
    ("origination_remote_payload_udp_address", INTEGER, NULLABLE, SKIP),
    ("origination_local_payload_ip_address", STRING, NULLABLE, SKIP),
    ("origination_local_payload_udp_address", INTEGER, NULLABLE, SKIP),
    ("origination_codec_list", STRING, NULLABLE, SKIP),
    ("origination_ingress_packets", INTEGER, NULLABLE, SKIP),
    ("origination_egress_packets", INTEGER, NULLABLE, SKIP),
    ("origination_ingress_octets", INTEGER, NULLABLE, SKIP),
    ("origination_egress_octets", INTEGER, NULLABLE, SKIP),
    ("origination_ingress_packet_loss", INTEGER, NULLABLE, SKIP),
    ("origination_ingress_delay", INTEGER, NULLABLE, SKIP),
    ("origination_ingress_packet_jitter", INTEGER, NULLABLE, SKIP),
    ("trunk_id_termination", STRING, NULLABLE, INCLUDE),
    ("voip_protocol_termination", STRING, NULLABLE, SKIP),
    ("termination_source_number", INTEGER, NULLABLE, INCLUDE),
    ("termination_source_host_name", STRING, NULLABLE, INCLUDE),
    ("termination_destination_number", INTEGER, NULLABLE, INCLUDE),
    ("termination_destination_host_name", STRING, NULLABLE, INCLUDE),
    ("termination_call_id", STRING, NULLABLE, INCLUDE),
    ("termination_remote_payload_ip_address", STRING, NULLABLE, SKIP),
    ("termination_remote_payload_udp_address", INTEGER, NULLABLE, SKIP),
    ("termination_local_payload_ip_address", STRING, NULLABLE, SKIP),
    ("termination_local_payload_udp_address", INTEGER, NULLABLE, SKIP),
    ("termination_codec_list", STRING, NULLABLE, SKIP),
    ("termination_ingress_packets", INTEGER, NULLABLE, SKIP),
    ("termination_egress_packets", INTEGER, NULLABLE, SKIP),
    ("termination_ingress_octets", INTEGER, NULLABLE, SKIP),
    ("termination_egress_octets", INTEGER, NULLABLE, SKIP),
    ("termination_ingress_packet_loss", INTEGER, NULLABLE, SKIP),
    ("termination_ingress_delay", INTEGER, NULLABLE, SKIP),
    ("termination_ingress_packet_jitter", INTEGER, NULLABLE, SKIP),
    ("final_route_indication", STRING, NULLABLE, INCLUDE),
    ("routing_digits", INTEGER, NULLABLE, INCLUDE),
    ("call_duration", INTEGER, NULLABLE, INCLUDE),
    ("pdd", INTEGER, NULLABLE, INCLUDE),
    ("ring_time", INTEGER, NULLABLE, INCLUDE),
    ("callduration_in_ms", INTEGER, NULLABLE, INCLUDE),
    ("conf_id", INTEGER, NULLABLE, INCLUDE),
    ("call_type", INTEGER, NULLABLE, SKIP),
    ("ingress_id", INTEGER, NULLABLE, SKIP),
    ("ingress_client_id", INTEGER, NULLABLE, INCLUDE),
    ("ingress_client_rate_table_id", INTEGER, NULLABLE, INCLUDE),
    ("ingress_client_currency_id", INTEGER, NULLABLE, SKIP),
    ("ingress_client_rate", FLOAT, NULLABLE, INCLUDE),
    ("ingress_client_currency", INTEGER, NULLABLE, SKIP),
    ("ingress_client_bill_time", INTEGER, NULLABLE, INCLUDE),
    ("ingress_client_bill_result", INTEGER, NULLABLE, SKIP),
    ("ingress_client_cost", FLOAT, NULLABLE, INCLUDE),
    ("egress_id", INTEGER, NULLABLE, INCLUDE),
    ("egress_rate_table_id", INTEGER, NULLABLE, INCLUDE),
    ("egress_rate", FLOAT, NULLABLE, INCLUDE),
    ("egress_cost", FLOAT, NULLABLE, INCLUDE),
    ("egress_bill_time", INTEGER, NULLABLE, INCLUDE),
    ("egress_client_id", INTEGER, NULLABLE, INCLUDE),
    ("egress_client_currency_id", INTEGER, NULLABLE, SKIP),
    ("egress_client_currency", STRING, NULLABLE, SKIP),
    ("egress_six_seconds", INTEGER, NULLABLE, SKIP),
    ("egress_bill_minutes", FLOAT, NULLABLE, INCLUDE),
    ("egress_bill_result", INTEGER, NULLABLE, SKIP),
    ("ingress_bill_minutes", FLOAT, NULLABLE, INCLUDE),
    ("ingress_dnis_type", INTEGER, NULLABLE, SKIP),
    ("ingress_rate_type", INTEGER, NULLABLE, INCLUDE),
    ("lrn_number_vendor", INTEGER, NULLABLE, SKIP),
    ("lrn_dnis", INTEGER, NULLABLE, INCLUDE),
    ("egress_dnis_type", INTEGER, NULLABLE, SKIP),
    ("egress_rate_type", INTEGER, NULLABLE, INCLUDE),
    ("item_id", INTEGER, NULLABLE, SKIP),
    ("translation_ani", INTEGER, NULLABLE, SKIP),
    ("ingress_rate_id", INTEGER, NULLABLE, SKIP),
    ("egress_rate_id", INTEGER, NULLABLE, SKIP),
    ("orig_code", INTEGER, NULLABLE, INCLUDE),
    ("orig_code_name", STRING, NULLABLE, INCLUDE),
    ("orig_country", STRING, NULLABLE, INCLUDE),
    ("term_code", INTEGER, NULLABLE, INCLUDE),
    ("term_code_name", STRING, NULLABLE, INCLUDE),
    ("term_country", STRING, NULLABLE, INCLUDE),
    ("ingress_rate_effective_date", INTEGER, NULLABLE, SKIP),
    ("egress_rate_effective_date", INTEGER, NULLABLE, SKIP),
    ("egress_erro_string", STRING, NULLABLE, SKIP),
    ("order_id", INTEGER, NULLABLE, SKIP),
    ("order_type", INTEGER, NULLABLE, SKIP),
    ("lnp_dipping_cost", FLOAT, NULLABLE, SKIP),
    ("is_final_call", INTEGER, NULLABLE, SKIP),
    ("egress_code_asr", FLOAT, NULLABLE, SKIP),
    ("egress_code_acd", FLOAT, NULLABLE, SKIP),
    ("static_route", STRING, NULLABLE, SKIP),
    ("dynamic_route", INTEGER, NULLABLE, SKIP),
    ("route_plan", INTEGER, NULLABLE, INCLUDE),
    ("route_prefix", STRING, NULLABLE, SKIP),
    ("orig_delay_second", INTEGER, NULLABLE, INCLUDE),
    ("term_delay_second", INTEGER, NULLABLE, INCLUDE),
    ("orig_call_duration", INTEGER, NULLABLE, SKIP),
    ("trunk_type", INTEGER, NULLABLE, SKIP),
    ("origination_profile_port", INTEGER, NULLABLE, SKIP),
    ("termination_profile_port", INTEGER, NULLABLE, SKIP),
    ("o_trunk_type2", INTEGER, NULLABLE, INCLUDE),
    ("o_billing_method", INTEGER, NULLABLE, SKIP),
    ("t_trunk_type2", INTEGER, NULLABLE, SKIP),
    ("t_billing_method", INTEGER, NULLABLE, SKIP),
    ("campaign_id", INTEGER, NULLABLE, SKIP),
    ("tax", INTEGER, NULLABLE, SKIP),
    ("agent_id", INTEGER, NULLABLE, SKIP),
    ("agent_rate", FLOAT, NULLABLE, SKIP),
    ("agent_cost", FLOAT, NULLABLE, SKIP),
    ("orig_jur_type", INTEGER, NULLABLE, SKIP),
    ("term_jur_type", INTEGER, NULLABLE, SKIP),
    ("ring_epoch", INTEGER, NULLABLE, SKIP),
    ("end_epoch", INTEGER, NULLABLE, SKIP),
    ("paid_user", STRING, NULLABLE, SKIP),
    ("rpid_user", STRING, NULLABLE, SKIP),
    ("timeout_type", INTEGER, NULLABLE, SKIP),
    ("DUMMY1", INTEGER, NULLABLE, SKIP),
    ("DUMMY2", STRING, NULLABLE, SKIP),
]

cdr_schema_fields = [
        {
            "name": name,
            "type": type,
            "mode": mode
        }
        for (name, type, mode, skip) in fields if not skip]

fields_to_include = [i for i, (_, _, _, skip) in enumerate(fields) if not skip]

def irow(filename):
    """
    iterate lines and process them accordingly:
    - skip fields that need to be skipped
    - replace "NULL" to None
    - replace delimmiter "?" to ','
    """
    ig = operator.itemgetter(*fields_to_include)
    with gzip.GzipFile(filename, "rb") if filename.endswith(".gz") else open(filename, "rb") as source_file:
        source = csv.reader(source_file, delimiter='?')
        for row in source:
            yield [None if v is None or v.upper() == "NULL" else v for v in ig(row)]


def convert_csv(filename):
    """
    Reads original CSV file, removes NULL, fields that should be skipped,
    and replaces '?' delimiters with ','

    Returns gzipped temporal file.
    """
    description = TemporaryFile()
    destination_csv = csv.writer(gzip.GzipFile(fileobj=description, mode="wb"))
    destination_csv.writerows(irow(filename))
    return description


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
                    "schema": {"fields": cdr_schema_fields},
                    'destinationTable': {
                        'projectId': project_id,
                        'datasetId': dataset_id,
                        'tableId': table_name
                    },
                    'sourceFormat': source_format,
                    #"fieldDelimiter": '?'
                }
            }
        },
        media_body=MediaIoBaseUpload( ##MediaFileUpload(
            ##data_path,
            fd=convert_csv(data_path),
            mimetype="application/x-gzip" if data_path.split('.')[-1] == "gz" else 'application/octet-stream')
    )
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
