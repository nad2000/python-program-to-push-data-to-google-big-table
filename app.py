import os
import logging
import urllib
import json
import csv
from cStringIO import StringIO
import tarfile
import gzip
from datetime import datetime

from google.appengine.ext import ndb
from google.appengine.api import users
import webapp2
from webapp2_extras import jinja2

from models import CDRFile, CDR

def set_trace():
    """Support for debugging GAE application.
    Alsternatively you can start debugging running:
    python -m pdb google_appengine/dev_appserver.py <app dir>
    """
    # ignore, if it's not "localhost"
    if os.environ["SERVER_NAME"] != "localhost":
        return
    import pdb, sys
    debugger = pndb.Pdb(
        stdin=sys.__stdin__,
        stdout=sys.__stdout__)
    debugger.set_trace(sys._getframe().f_back)


class RequestHandler(webapp2.RequestHandler):
    """Application Request Handler base. Place for implementation
    of common functions of the application handlers."""

    @webapp2.cached_property
    def jinja2(self):
        # Returns a Jinja2 renderer cached in the app registry
        return jinja2.get_jinja2(app=self.app)

    def render(self, template_name, template_values=None, debug=False):
        """Renders the template with the given dict of values and outputs
        the rendered content to the response.

        Example usage:
            render("index.html", {"name": "Robert Hunt", "values": [1, 2, 3]})

        Arguments:
            template_name: a name of a Jinja2 template
            template_values: dictionary of values to apply to the template
            debug: debugging flag
        """
        if not(template_values):
            template_values = {}
        # Renders a template and writes the result to the response.
        rv = self.jinja2.render_template(template_name, template_values)
        self.response.write(rv)


class MainPage(RequestHandler):
    """Application default handler."""
    def get(self):
        """Redirects to the static page."""
        self.redirect("index.html")


class PyInfoHandler(RequestHandler):
    """Handler outputs information about the current state of the environment."""
    def get(self):
        res = self.response
        req = self.request
        res.headers["Content-Type"] = "text/plain"
        res.out.write("""<html><head></head><body><h1>Environment</h1>""")
        for name in os.environ.keys():
            res.out.write("%s = %s<br />\n" % (name, os.environ[name]))
        res.out.write("\n\n<h1>Headers</h1>\n")
        for k, v in req.headers.items():
            res.out.write("%s = %s<br />\n" % (k, v))

    def put(self):
        res = self.response
        req = self.request
        CDRFile.load_file(req.GET.get("filename"), req.body_file_raw)
        self.get()
        res.out.write("\n\n<h1>Request:</h1>\n")
        for f in sorted((dir(req))):
            res.out.write("%s<br/>\n" % f)

    def post(self):
        res = self.response
        req = self.request
        CDRFile.load_file(req.POST.get("file").filename, req.POST.get("file").file)
        self.get()
        res.out.write("\n\n<h1>Request:</h1>\n")
        for f in sorted((dir(req))):
            res.out.write("%s<br/>\n" % f)
        res.out.write("\n\n<h1>POST:</h1>\n")
        for f in sorted(req.POST.keys()):
            res.out.write("%s: %s<br/>\n" % (f, dir(req.POST[f])))

class CDRFileHandler(RequestHandler):

    def get(self):
        res = self.response
        rows = ndb.gql("SELECT filename, row_count, size FROM CDRFile")
        res.headers["Content-Type"] = "application/json"
        json.dump([r.to_dict() for r in rows], res.out, indent=4)

class CDRHandler(RequestHandler):

    def upload(self, filename, file_obj):
        res = self.response
        req = self.request
        switch = req.GET.get("switch")
        if ".tar" in filename:
            row_count = 0
            compression = filename.split('.')[-1]
            mode = 'r'
            if compression in ["gz", "bz2"]:
                mode += ':' + compression
            with tarfile.open(fileobj=file_obj, mode=mode) as tf:
                for ti in tf.getmembers():
                    logging.debug("*** EXTRACTED FILE: %s", ti.path)
                    row_count += CDRFile.load_file(os.path.basename(ti.path), tf.extractfile(ti))
        else:
            row_count = CDRFile.load_file(filename, file_obj)
        res.headers["Content-Type"] = "application/json"
        json.dump(dict(switch=switch, filename=filename, row_count=row_count), res.out)
        return row_count

    def get(self, filename=None):
        res = self.response
        req = self.request
        if filename is None:
            filename = req.GET.get("filename")
        params = {p: req.GET[p] for p in req.GET if p in CDR.get_fields()}
        rows = CDRFile.get_rows(filename=filename, **params)
        if "json" in req.GET:
            res.headers["Content-Type"] = "application/json"
            json.dump([r.to_dict() for r in rows], res.out, indent=4)
        else:
            output_name = (filename if filename is not None else datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S") + ".csv") + ".gz"
            res.headers["Content-Type"] = "application/x-gzip"
            res.headers["Content-Endoding"] = "gzip"
            res.headers["Content-Disposition"] = 'attachment; filename="{}"'.format(output_name)
            with gzip.GzipFile(mode="wb", compresslevel=9, fileobj=res.out) as gf:
                cw = csv.writer(gf)
                cw.writerow(CDR.get_fields())
                for r in rows:
                    cw.writerow(r.to_dict().values())

    def put(self, filename=None):
        req = self.request
        if filename is None:
            filename = req.GET.get("filename")
        self.upload(filename, req.body_file_raw)

    def post(self, filename=None):
        req = self.request
        fs = req.POST.get("file")
        if filename is None:
            filename = fs.filename
        logging.debug("FILE: %s, CONTENT TYPE: %s", filename, fs.type)
        self.upload(filename, fs.file)

    def delete(self, filename=None):
        res = self.response
        req = self.request
        if filename is None:
            filename = reqt.GET.get("filename")
        row_count = CDRFile.delete(filename=filename)
        res.headers["Content-Type"] = "application/json"
        json.dump(dict(filename=filename, row_count=row_count), res.out)


class UploadHandler(RequestHandler):
    """File upload handler."""

    def initialize(self, request, response):
        """webapp2.RequestHandler handler initialization extension."""
        super(UploadHandler, self).initialize( request, response)
        response.headers.add_header("Pragma", "no-cache")
        response.headers.add_header("Cache-Control", "private, no-cache")
        response.headers.add_header("Content-Disposition", "inline", filename="files.json")
        response.headers.add_header("X-Content-Type-Options", "nosniff")

    def get_file_objects(self, filename=None):
        """Creats file object for including into JSON feed."""
        file_qry = CDRFile.query().order(-CDRFile.date)
        if filename:
            file_qry.filter(CDRFile.filename == filename)
        return [{
            "name": f.filename,
            "type": f.mimetype,
            "size": f.size,
            "url": f.url,
            "row_count": f.row_count,
            #"delete_url": self.request.path + '?' + urllib.urlencode({"file": f.filename}),
            "delete_url": "delete?" + urllib.urlencode({"file": f.filename}),
            "delete_type": "DELETE"
        } for f in file_qry]

        def head(self):
            self.get()

    def get(self):
        """Retrieves file list or a single file info"""
        filename = self.request.get("file")
        if filename:
            info = self.get_file_objects(filename)
        else:
            info = self.get_file_objects()
        self.response.headers.add_header("Content-type", "application/json")
        json.dump(info, self.response.out)

    def post(self):

        filename = self.request.POST.get("files[]").filename
        content = self.request.get("files[]")

        if content:
            load_cdr(filename, StringIO(content))

        json.dump([{
            "name": filename,
            "type": mimetype,
            "size": size,
            "url": uf.url,
            "row_count": row_count,
            ##"delete_url": self.request.path + '?' + urllib.urlencode({"file": filename}),
            "delete_url": 'delete?' + urllib.urlencode({"file": filename}),
            "delete_type": "DELETE"
        }] , self.response.out )

    def delete(self):
        """Delete file form the store."""
        filename = self.request.get("file")
        success = (filename != None)
        if success:
            CDRFile.delete(filename)
        self.response.headers.add_header("Content-type", "application/json")
        json.dump(success, self.response.out)

app = webapp2.WSGIApplication([
        ('/', MainPage),
        ("/cdrfile", CDRFileHandler),
        ("/cdr", CDRHandler),
        ("/cdr/(.+)", CDRHandler),
        ("/upload", UploadHandler),
        ("/delete", UploadHandler),
        ("/pyinfo", PyInfoHandler)],
    debug=True)

