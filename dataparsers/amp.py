import sys, time

sys.path.append("..")
from database import Database 

def icmp_stream_table():
    from sqlalchemy import Column
    from sqlalchemy.types import String, Integer

    return [
        Column('source', String, nullable=False),
        Column('destination', String, nullable=False),
        Column('packetsize', String, nullable=False),
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

def icmp_insert_stream(db, source, dest, size, start, name):
    db.insert_stream(mod="amp", modsubtype="icmp", name=name, source=source,
            destination=dest, packetsize=size, lasttimestamp=start)

def icmp_stream_constraints():
    return ['source', 'destination', 'packetsize']

def icmp_parse_db_row(row):
    pass

class AmpModule:
    def __init__(self, tests, config):
        ts = int(time.mktime(time.localtime()))

        self.icmp_tests = {}

        for i in tests:
            if i['lasttimestamp'] == 0:
                start = ts - 120
                start = start - (start % 60)
                start -= 30
                i['lasttimestamp'] = start
            elif (i['lasttimestamp'] % 60) != 30:
                i['lasttimestamp'] -= (i['lasttimestamp'] % 60)
                i['lasttimestamp'] -= 30

            if i['modsubtype'] == 'icmp':
                self.icmp_tests[i['stream_id']] = i

        # TODO: Connect to rabbitmq

    def run(self):
        print self.icmp_tests[0]
        while True:
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
    start = None

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
        if x[0] == 'startts':
            start = x[1]
        if x[0] == 'test':
            test = x[1]
        if x[0] == 'subtype':
            subtype = x[1]
        if name == None or source == None or target == None:
            continue
        if subtype == None or start == None or test == None:
            continue

        if start[0] == '-':
            period = start[-1]
            num = int(start[1:-1])

            if period == 's':
                subtract = num
            elif period == 'm':
                subtract = num * 60
            elif period == 'h':
                subtract = num * 60 * 60
            elif period == 'd':
                subtract = num * 60 * 60 * 24
            elif period == 'w':
                subtract = num * 60 * 60 * 24 * 7
            else:
                print >> sys.stderr, "Invalid period for startts: %s" % (period)
                continue

            now = int(time.mktime(time.gmtime()))
            startts = now - subtract
        else:
            startts = start

        if test == "icmp":
            icmp_insert_stream(db, source, target, subtype, startts, name)

        name = None
        source = None
        target = None
        test = None
        subtype = None
        start = None


def run_module(tests, config):

	amp = AmpModule(tests, config)
	amp.run()

def tables():
    res = {}
    res["amp_icmp"] = (icmp_stream_table(), icmp_stream_constraints(), icmp_data_table())

    return res


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
