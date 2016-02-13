#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import sys
import os

ENTRY_POINT = os.environ.get("ENTRY_POINT", "http://localhost:8080")

def upload_file(filename, switch=None):
    url = ENTRY_POINT
    if not url.endswith("/cdr"):
        url += "/cdr"
    if switch is not None:
        url += "?switch=" + requests.utils.quote(switch)
    res = requests.post(
            url,
            files={"file": open(filename, "rb")})

if __name__ == "__main__":
    upload_file(sys.argv[1])
