import sys, time

from libnntsc.database import Database 
from libnntsc.configurator import *

def icmp_stream_table():
    from sqlalchemy import Column
    from sqlalchemy.types import String, Integer

    return [
        Column('source', String, nullable=False),
        Column('destination', String, nullable=False),
        Column('packetsize', String, nullable=False),
        Column('datastyle', String, nullable=False),
        Column('lasttimestamp', Integer, nullable=False, default=0)
    ]

def icmp_data_table():
    from sqlalchemy import Column
    from sqlalchemy.types import String, Integer, Boolean

    return [
        Column('packetsize', Integer, nullable=False),
        Column('rtt', Integer, nullable=False),
        Column('loss', Boolean, nullable=False),
        Column('errortype', Integer, nullable=False),
        Column('errorcode', Integer, nullable=False)
    ]

def icmp_insert_stream(db, source, dest, size, name):
    db.insert_stream(mod="amp", modsubtype="icmp", name=name, 
            source=source, destination=dest, packetsize=size, 
            datastyle="rtt_ms", lasttimestamp=0)

def icmp_stream_constraints():
    return ['source', 'destination', 'packetsize']

def icmp_parse_db_row(row):
    pass

class AmpModule:
    def __init__(self, tests, nntsc_config, exp):
        ts = int(time.mktime(time.localtime()))

        self.exporter = exp
        self.icmp_tests = {}

        for i in tests:

            if i['modsubtype'] == 'icmp':
                self.icmp_tests[i['stream_id']] = i


        # TODO: Connect to rabbitmq

    def run(self):
 
        while True:
            time.sleep(120)
            pass
            # Get result from our message queue

            # Check the test type

            # Based on that, call the right function for db insertion


def insert_amp_streams(db, conf):

    if conf == "":
        return

    try:
        f = open(conf, "r")
    except IOError, e:
        print >> sys.stderr, "WARNING: %s does not exist - no AMP streams will be added" % (conf)
        print >> sys.stderr, e
        return

    name = None
    source = None
    target = None
    test = None
    subtype = None

    for line in f:
        if line[0] == '#':
            continue
        if line == "\n" or line == "":
            continue

        x = line.strip().split("=")
        if len(x) != 2:
            continue
        x[0] = x[0].strip()
        x[1] = x[1].strip()
        if x[0] == 'name':
            name = x[1]
        if x[0] == 'source':
            source = x[1]
        if x[0] == 'target':
            target = x[1]
        if x[0] == 'test':
            test = x[1]
        if x[0] == 'subtype':
            subtype = x[1]
        if name == None or source == None or target == None:
            continue
        if subtype == None or test == None:
            continue

        if test == "icmp":
            icmp_insert_stream(db, source, target, subtype, name)

        name = None
        source = None
        target = None
        test = None
        subtype = None


def run_module(tests, config, exp):

	amp = AmpModule(tests, config, exp)
	amp.run()

def tables():
    res = {}
    res["amp_icmp"] = (icmp_stream_table(), icmp_stream_constraints(), icmp_data_table())

    return res


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
