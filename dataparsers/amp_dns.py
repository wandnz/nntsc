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


from sqlalchemy import create_engine, Table, Column, Integer, \
        String, MetaData, ForeignKey, UniqueConstraint, Index
from sqlalchemy.types import Integer, String, Float, Boolean, SmallInteger
from sqlalchemy.exc import DataError, IntegrityError, OperationalError, \
        SQLAlchemyError, ProgrammingError
from sqlalchemy.dialects import postgresql
from libnntsc.database import DB_DATA_ERROR, DB_GENERIC_ERROR, DB_NO_ERROR
import libnntscclient.logger as logger

STREAM_TABLE_NAME = "streams_amp_dns"
DATA_TABLE_NAME = "data_amp_dns"

amp_dns_streams = {}

streamkeys = ['destination', 'instance', 'address', 'query', 'query_type',
    'query_class', 'udp_payload_size', 'recurse', 'dnssec', 'nsid', 'source']
flagnames = ['rd', 'tc', 'aa', 'qr', 'cd', 'ad', 'ra']

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

    props = {}

    for k,v in data.items():
        if k in streamkeys:
            props[k] = v

    return db.insert_stream(exp, STREAM_TABLE_NAME, DATA_TABLE_NAME,
            "amp", "dns", name, timestamp, props)

def stream_table(db):
    if STREAM_TABLE_NAME in db.metadata.tables:
        return STREAM_TABLE_NAME

    st = Table(STREAM_TABLE_NAME, db.metadata,
        Column('stream_id', Integer, ForeignKey("streams.id"),
            primary_key=True),
        Column('source', String, nullable=False),
        Column('destination', String, nullable=False),
        Column('instance', String, nullable=False),
        Column('address', postgresql.INET, nullable=False),
        Column('query', String, nullable=False),
        Column('query_type', String, nullable=False),
        Column('query_class', String, nullable=False),
        Column('udp_payload_size', Integer, nullable=False),
        Column('recurse', Boolean, nullable=False),
        Column('dnssec', Boolean, nullable=False),
        Column('nsid', Boolean, nullable=False),
        UniqueConstraint('source', 'destination', 'address', 'query', 'query_type',
                'query_class', 'udp_payload_size', 'recurse', 'dnssec', 'nsid',
                'instance'),
        useexisting=True,
    )

    Index('index_amp_dns_source', st.c.source)
    Index('index_amp_dns_destination', st.c.destination)
    Index('index_amp_dns_query', st.c.query)

    return STREAM_TABLE_NAME

def data_table(db):
    if DATA_TABLE_NAME in db.metadata.tables:
        return DATA_TABLE_NAME

    dt = Table(DATA_TABLE_NAME, db.metadata,
        Column('stream_id', Integer, nullable=False),
        Column('timestamp', Integer, nullable=False),
        Column('response_size', Integer, nullable=True),
        Column('rtt', Integer, nullable=True),
        Column('ttl', SmallInteger, nullable=True),
        Column('query_len', SmallInteger, nullable=False),
        Column('total_answer', SmallInteger, nullable=True),
        Column('total_authority', SmallInteger, nullable=True),
        Column('total_additional', SmallInteger, nullable=True),
        Column('opcode', SmallInteger, nullable=True),
        Column('rcode', SmallInteger, nullable=True),
        Column('flag_rd', Boolean, nullable=True),
        Column('flag_tc', Boolean, nullable=True),
        Column('flag_aa', Boolean, nullable=True),
        Column('flag_qr', Boolean, nullable=True),
        Column('flag_cd', Boolean, nullable=True),
        Column('flag_ad', Boolean, nullable=True),
        Column('flag_ra', Boolean, nullable=True),
    )

    Index('index_amp_dns_timestamp', dt.c.timestamp)
    Index('index_amp_dns_rtt', dt.c.rtt)

    return DATA_TABLE_NAME


def insert_data(db, exp, stream, ts, result):
    return db.insert_data(exp, DATA_TABLE_NAME, "amp_dns", stream, ts, result)


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

        res = insert_data(db, exp, stream_id, timestamp, dataresult)
        if res != DB_NO_ERROR:
            return res
        done[stream_id] = 0

    return db.update_timestamp(done.keys(), timestamp)


def register(db):
    st_name = stream_table(db)
    dt_name = data_table(db)

    db.register_collection("amp", "dns", st_name, dt_name)


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
