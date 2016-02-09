import sys
sys.path.insert(1, "../google_appengine")
sys.path.insert(1, "../google_appengine/lib/yaml/lib")

import pytest
import webtest

from google.appengine.api import memcache
from google.appengine.ext import ndb
from google.appengine.ext import testbed

import main


@pytest.fixture
def testapp():
    return webtest.TestApp(main.app)


def test_main(testapp):
    res = testapp.get("/main")
    assert res.status_int == 200
    assert res.content_type == "text/plain"
    assert res.normal_body == "Hello, World!"

