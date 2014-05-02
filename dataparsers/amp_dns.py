# This file is part of NNTSC
#
# Copyright (C) 2013 The University of Waikato, Hamilton, New Zealand
# Authors: Shane Alcock
#          Brendon Jones
#          Nathan Overall
#
# All rights reserved.
#
# This code has been developed by the WAND Network Research Group at the
# University of Waikato. For more information, please see
# http://www.wand.net.nz/
#
# This source code is proprietary to the University of Waikato and may not be
# redistributed, published or disclosed without prior permission from the
# University of Waikato and the WAND Network Research Group.
#
# Please report any bugs, questions or comments to contact@wand.net.nz
#
# $Id$


from libnntsc.dberrorcodes import *
import libnntscclient.logger as logger
from libnntsc.parsers.common import create_new_stream, insert_data

STREAM_TABLE_NAME = "streams_amp_dns"
DATA_TABLE_NAME = "data_amp_dns"
COLNAME = "amp_dns"

amp_dns_streams = {}

streamkeys = ['destination', 'instance', 'address', 'query', 'query_type',
    'query_class', 'udp_payload_size', 'recurse', 'dnssec', 'nsid', 'source']
flagnames = ['rd', 'tc', 'aa', 'qr', 'cd', 'ad', 'ra']

dns_datacols = [ \
    {"name":"response_size", "type":"integer", "null":True},
    {"name":"rtt", "type":"integer", "null":True},
    {"name":"ttl", "type":"smallint", "null":True},
    {"name":"query_len", "type":"smallint", "null":True},
    {"name":"total_answer", "type":"smallint", "null":True},
    {"name":"total_authority", "type":"smallint", "null":True},
    {"name":"total_additional", "type":"smallint", "null":True},
    {"name":"opcode", "type":"smallint", "null":True},
    {"name":"rcode", "type":"smallint", "null":True},
    {"name":"flag_rd", "type":"boolean", "null":True},
    {"name":"flag_tc", "type":"boolean", "null":True},
    {"name":"flag_aa", "type":"boolean", "null":True},
    {"name":"flag_qr", "type":"boolean", "null":True},
    {"name":"flag_cd", "type":"boolean", "null":True},
    {"name":"flag_ad", "type":"boolean", "null":True},
    {"name":"flag_ra", "type":"boolean", "null":True},
]

dns_streamcols = [ \
        {"name":"source", "type":"varchar", "null":False},
        {"name":"destination", "type":"varchar", "null":False},
        {"name":"instance", "type":"varchar", "null":False},
        {"name":"address", "type":"inet", "null":False},
        {"name":"query", "type":"varchar", "null":False},
        {"name":"query_type", "type":"varchar", "null":False},
        {"name":"query_class", "type":"varchar", "null":False},
        {"name":"udp_payload_size", "type":"integer", "null":False},
        {"name":"recurse", "type":"boolean", "null":False},
        {"name":"dnssec", "type":"boolean", "null":False},
        {"name":"nsid", "type":"boolean", "null":False}
    ]


def result_to_key(res):
    key = (str(res['source']), str(res['destination']), str(res['instance']),
            res['address'], str(res['query']), str(res['query_type']),
            str(res['query_class']), str(res['udp_payload_size']),
            res['recurse'], res['dnssec'], res['nsid'])

    return key

def create_existing_stream(s):

    key = result_to_key(s)
    amp_dns_streams[key] = s['stream_id']

def insert_stream(db, exp, data, timestamp):
    name = "dns %s:%s:%s" % (data['source'], data['destination'], data['query'])

    return create_new_stream(db, exp, "amp", "dns", name, dns_streamcols,
            data, timestamp, STREAM_TABLE_NAME, DATA_TABLE_NAME)

def stream_table(db):
    
    uniqcols = ['source', 'destination', 'query', 'address', 'query_type',
            'query_class', 'udp_payload_size', 'recurse', 'dnssec', 'nsid',
            'instance']

    err = db.create_streams_table(STREAM_TABLE_NAME, dns_streamcols, uniqcols)
    if err != DB_NO_ERROR:
        logger.log("Failed to create streams table for amp-icmp")
        return None

    err = db.create_index("", STREAM_TABLE_NAME, ['source'])
    if err != DB_NO_ERROR:
        logger.log("Failed to create source index on %s" % (STREAM_TABLE_NAME))
        return None

    err = db.create_index("", STREAM_TABLE_NAME, ['destination'])
    if err != DB_NO_ERROR:
        logger.log("Failed to create dest index on %s" % (STREAM_TABLE_NAME))
        return None

    err = db.create_index("", STREAM_TABLE_NAME, ['query'])
    if err != DB_NO_ERROR:
        logger.log("Failed to create query index on %s" % (STREAM_TABLE_NAME))
        return None

    return STREAM_TABLE_NAME

def data_table(db):

    indexes = [{"columns":['rtt']}]

    err = db.create_data_table(DATA_TABLE_NAME, dns_datacols, indexes)
    if err != DB_NO_ERROR:
        return None


    return DATA_TABLE_NAME


def split_result(alldata, result):

    stream = {}
    data = {}

    for k, v in alldata.items():
        if k == "results":
            continue
        stream[k] = v

    for k, v in result.items():
        if k in streamkeys:
            stream[k] = v

        elif k == "flags":
            for f, fval in v.items():
                if f in flagnames:
                    data["flag_" + f] = fval
                else:
                    data[f] = fval
        else:
            data[k] = v

    return stream, data

def process_data(db, exp, timestamp, data, source):
    done = {}

    for r in data['results']:
        streamresult, dataresult = split_result(data, r)

        # Source is not part of the AMP result itself
        streamresult['source'] = source

        key = result_to_key(streamresult)
        if key in amp_dns_streams:
            stream_id = amp_dns_streams[key]

            if stream_id in done:
                continue
        else:
            stream_id = insert_stream(db, exp, streamresult, timestamp)
            if stream_id < 0:
                logger.log("AMPModule: Cannot create stream for:")
                logger.log("AMPModule: %s %s %s %s\n" % ("dns", source,
                        streamresult['destination'], streamresult['query']))
                return stream_id
            amp_dns_streams[key] = stream_id

        res = insert_data(db, exp, stream_id, timestamp, dataresult, 
                dns_datacols, COLNAME, DATA_TABLE_NAME)
        if res != DB_NO_ERROR:
            return res
        done[stream_id] = 0

    return db.update_timestamp(done.keys(), timestamp)


def register(db):
    st_name = stream_table(db)
    dt_name = data_table(db)

    if st_name == None or dt_name == None:
        logger.log("Error creating AMP DNS base tables")
        return DB_CODING_ERROR

    return db.register_collection("amp", "dns", st_name, dt_name)


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
