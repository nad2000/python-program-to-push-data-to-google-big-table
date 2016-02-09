import logging
import csv

from google.appengine.ext import ndb
from google.appengine.api import users


def to_int(str_val):
    if str_val is None or (type(str_val) is str and str_val.upper() == "NULL"):
        return None
    else:
        return int(str_val)

def to_float(str_val):
    if str_val is None or (type(str_val) is str and str_val.upper() == "NULL"):
        return None
    else:
        return float(str_val)

class Switch(ndb.Model):
    name = ndb.StringProperty(indexed=True)
    ip_addr = ndb.StringProperty()
    public_ip_addr = ndb.StringProperty()



class CDR(ndb.Model):
    #cdr_file = ndb.KeyProperty()

    release_cause = ndb.IntegerProperty()
    start_time_of_date = ndb.IntegerProperty()
    answer_time_of_date = ndb.IntegerProperty()
    release_cause_from_protocol_stack = ndb.StringProperty()
    binary_value_of_release_cause_from_protocol_stack = ndb.StringProperty()
    trunk_id_origination = ndb.StringProperty(indexed=True)
    origination_source_number = ndb.IntegerProperty()
    origination_source_host_name = ndb.StringProperty()
    origination_destination_number = ndb.IntegerProperty()
    origination_destination_host_name = ndb.StringProperty()
    origination_call_id = ndb.StringProperty()
    trunk_id_termination = ndb.StringProperty(indexed=True)
    termination_source_number = ndb.IntegerProperty()
    termination_source_host_name = ndb.StringProperty()
    termination_destination_number = ndb.IntegerProperty()
    termination_destination_host_name = ndb.StringProperty()
    termination_call_id = ndb.StringProperty()
    final_route_indication = ndb.StringProperty()
    routing_digits = ndb.IntegerProperty()
    call_duration = ndb.IntegerProperty(indexed=True)
    pdd = ndb.IntegerProperty()
    ring_time = ndb.IntegerProperty()
    callduration_in_ms = ndb.IntegerProperty()
    conf_id = ndb.IntegerProperty()
    ingress_client_id = ndb.IntegerProperty()
    ingress_client_rate_table_id = ndb.IntegerProperty()
    ingress_client_rate = ndb.FloatProperty()
    ingress_client_bill_time = ndb.IntegerProperty()
    ingress_client_cost = ndb.FloatProperty()
    egress_id = ndb.IntegerProperty()
    egress_rate_table_id = ndb.IntegerProperty()
    egress_rate = ndb.FloatProperty()
    egress_cost = ndb.FloatProperty()
    egress_bill_time = ndb.IntegerProperty()
    egress_client_id = ndb.IntegerProperty()
    egress_bill_minutes = ndb.FloatProperty()
    ingress_bill_minutes = ndb.FloatProperty()
    ingress_rate_type = ndb.IntegerProperty()
    lrn_dnis = ndb.IntegerProperty()
    egress_rate_type = ndb.IntegerProperty()
    orig_code = ndb.IntegerProperty()
    orig_code_name = ndb.StringProperty()
    orig_country = ndb.StringProperty()
    term_code = ndb.IntegerProperty()
    term_code_name = ndb.StringProperty()
    term_country = ndb.StringProperty()
    route_plan = ndb.IntegerProperty()
    orig_delay_second = ndb.IntegerProperty()
    term_delay_second = ndb.IntegerProperty()


class CDRFile(ndb.Model):
    """
    Models an individual uploaded file.
    """
    #switch = ndb.KeyProperty()

    user = ndb.UserProperty()
    filename = ndb.StringProperty()
    size = ndb.IntegerProperty()
    url = ndb.StringProperty()
    mimetype = ndb.StringProperty()
    date = ndb.DateTimeProperty(auto_now_add=True)
    row_count = ndb.IntegerProperty()

    @classmethod
    def get_rows(cls, filename):
        cdrf = CDRFile.query(CDRFile.filename == filename).fetch()
        if cdrf == []:
            return []
        return CDR.query(ancestor=cdrf[0].key)

    @classmethod
    def delete(cls, filename):
        row_count = 0
        for f in CDRFile.query(CDRFile.filename == filename):
            row_count += f.row_count
            for rk in CDR.query(ancestor=f.key).iter(keys_only=True):
                rk.delete()
            logging.info("DELETING %d ROWS LOADES FROM '%s'", f.row_count, f.filename)
            f.key.delete()
        return row_count

    @classmethod
    def load_file(cls, filename, cdr_file):
        """
        Load CSV ('?' separated value file) into NDB

        @filename: file name
        @cdr_file: file object
        """

        logging.info("LOADING CDR FILE NAME: %s", filename)

        uf = cls(
            user=users.get_current_user(),
            filename=filename)
        uf.put()

        row_count = 0
        for row in csv.reader(cdr_file, delimiter='?'):
            (release_cause, start_time_of_date, answer_time_of_date, _, _,
            release_cause_from_protocol_stack,
            binary_value_of_release_cause_from_protocol_stack, _,
            trunk_id_origination, _, origination_source_number,
            origination_source_host_name, origination_destination_number,
            origination_destination_host_name, origination_call_id, _, _,
            _, _, _, _, _, _, _, _, _, _, trunk_id_termination, _,
            termination_source_number, termination_source_host_name,
            termination_destination_number,
            termination_destination_host_name, termination_call_id, _, _,
            _, _, _, _, _, _, _, _, _, _, final_route_indication,
            routing_digits, call_duration, pdd, ring_time,
            callduration_in_ms, conf_id, _, _, ingress_client_id,
            ingress_client_rate_table_id, _, ingress_client_rate, _,
            ingress_client_bill_time, _, ingress_client_cost, egress_id,
            egress_rate_table_id, egress_rate, egress_cost,
            egress_bill_time, egress_client_id, _, _, _,
            egress_bill_minutes, _, ingress_bill_minutes, _,
            ingress_rate_type, _, lrn_dnis, _, egress_rate_type, _, _, _,
            _, orig_code, orig_code_name, orig_country, term_code,
            term_code_name, term_country, _, _, _, _, _, _, _, _, _, _, _,
            route_plan, _, orig_delay_second, term_delay_second,) = row[2:108]

            cdr = CDR(
                parent=uf.key,
                release_cause=to_int(release_cause),
                start_time_of_date=to_int(start_time_of_date),
                answer_time_of_date=to_int(answer_time_of_date),
                release_cause_from_protocol_stack=release_cause_from_protocol_stack,
                binary_value_of_release_cause_from_protocol_stack=binary_value_of_release_cause_from_protocol_stack,
                trunk_id_origination=trunk_id_origination,
                origination_source_number=to_int(origination_source_number),
                origination_source_host_name=origination_source_host_name,
                origination_destination_number=to_int(origination_destination_number),
                origination_destination_host_name=origination_destination_host_name,
                origination_call_id=origination_call_id,
                trunk_id_termination=trunk_id_termination,
                termination_source_number=to_int(termination_source_number),
                termination_source_host_name=termination_source_host_name,
                termination_destination_number=to_int(termination_destination_number),
                termination_destination_host_name=termination_destination_host_name,
                termination_call_id=termination_call_id,
                final_route_indication=final_route_indication,
                routing_digits=to_int(routing_digits),
                call_duration=to_int(call_duration),
                pdd=to_int(pdd),
                ring_time=to_int(ring_time),
                callduration_in_ms=to_int(callduration_in_ms),
                conf_id=to_int(conf_id),
                ingress_client_id=to_int(ingress_client_id),
                ingress_client_rate_table_id=to_int(ingress_client_rate_table_id),
                ingress_client_rate=to_float(ingress_client_rate),
                ingress_client_bill_time=to_int(ingress_client_bill_time),
                ingress_client_cost=to_float(ingress_client_cost),
                egress_id=to_int(egress_id),
                egress_rate_table_id=to_int(egress_rate_table_id),
                egress_rate=to_float(egress_rate),
                egress_cost=to_float(egress_cost),
                egress_bill_time=to_int(egress_bill_time),
                egress_client_id=to_int(egress_client_id),
                egress_bill_minutes=to_float(egress_bill_minutes),
                ingress_bill_minutes=to_float(ingress_bill_minutes),
                ingress_rate_type=to_int(ingress_rate_type),
                lrn_dnis=to_int(lrn_dnis),
                egress_rate_type=to_int(egress_rate_type),
                orig_code=to_int(orig_code),
                orig_code_name=orig_code_name,
                orig_country=orig_country,
                term_code=to_int(term_code),
                term_code_name=term_code_name,
                term_country=term_country,
                route_plan=to_int(route_plan),
                orig_delay_second=to_int(orig_delay_second),
                term_delay_second=to_int(term_delay_second))
            cdr.put()
            row_count += 1

        uf.row_count = row_count
        uf.size = cdr_file.tell()
        uf.put()

        return row_count
