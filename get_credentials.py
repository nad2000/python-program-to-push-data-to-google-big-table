#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: ai ts=4 sts=4 et sw=4 ft=python

from __future__ import print_function

import httplib2
import pprint
import sys
import os

from apiclient.discovery import build
from apiclient.errors import HttpError

from oauth2client.client import OAuth2WebServerFlow, OOB_CALLBACK_URN
from oauth2client.client import AccessTokenRefreshError
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client import client

# Enter your Google Developer Project number OR Project ID
# (you can find it from Google Coud console):
PROJECT_NUMBER = "cdrstore-1216"
FLOW = flow_from_clientsecrets(
        os.path.join(os.path.dirname(__file__), "app-key.json"),
        scope="https://www.googleapis.com/auth/bigquery")


def main():

    storage = Storage(os.path.join(
        os.path.dirname(__file__), "app-credentials.json"))
    credentials = storage.get()
    http = httplib2.Http()

    if credentials is None or credentials.invalid:
        FLOW.redirect_uri = OOB_CALLBACK_URN
        authorize_url = FLOW.step1_get_authorize_url()
        print('Go to the following link in your browser:')
        print()
        print('    ' + authorize_url)
        print()
        code = raw_input("Enter verification code: ").strip()

        try:
            credentials = FLOW.step2_exchange(code, http=http)
        except client.FlowExchangeError as e:
            sys.exit('Authentication has failed: %s' % e)

        storage.put(credentials)
        credentials.set_store(storage)
        print('Authentication successful.')

    http_auth = credentials.authorize(http)

    bigquery_service = build("bigquery", "v2", http=http_auth)

    try:
        datasets = bigquery_service.datasets()
        listReply = datasets.list(projectId=PROJECT_NUMBER).execute()
        print("Dataset list:")
        pprint.pprint(listReply)

    except HttpError as err:
        print("Error in listDatasets:")
        pprint.pprint(err.content)

    except AccessTokenRefreshError:
        print("Credentials have been revoked or expired, please re-run"
              "the application to re-authorize")

if __name__ == "__main__":
    main()

