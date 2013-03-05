import sys

from libnntsc.database import Database
from libnntsc.configurator import *
import pika
from ampsave.importer import import_data_functions

def icmp_stream_table():
    """ Specify the description of an icmp stream, used to create the table """
    from sqlalchemy import Column
    from sqlalchemy.types import String, Integer

    return [
        Column('source', String, nullable=False),
        Column('destination', String, nullable=False),
        Column('packet_size', String, nullable=False),
        Column('datastyle', String, nullable=False),
        # XXX is last timestamp required any more?
        Column('lasttimestamp', Integer, nullable=False, default=0)
    ]

def icmp_data_table():
    """ Specify the description of icmp data, used to create the table """
    from sqlalchemy import Column
    from sqlalchemy.types import Integer, Boolean
    from sqlalchemy.dialects import postgresql

    return [
        Column('address', postgresql.INET, nullable=False),
        Column('packet_size', Integer, nullable=False),
        Column('rtt', Integer, nullable=False),
        Column('ttl', Integer, nullable=False),
        Column('loss', Boolean, nullable=False),
        Column('error_type', Integer, nullable=False),
        Column('error_code', Integer, nullable=False)
    ]

def icmp_insert_stream(db, source, dest, size, name):
    """ Insert a new icmp stream into the icmp stream table """
    return db.insert_stream(mod="amp", modsubtype="icmp", name=name,
            source=source, destination=dest, packet_size=size,
            datastyle="rtt_ms", lasttimestamp=0)

def icmp_stream_constraints():
    """ Specify the combination of columns that form the stream table key """
    return ['source', 'destination', 'packet_size']

def icmp_parse_db_row(row):
    pass

class AmpModule:
    def __init__(self, tests, nntsc_config, exp):

        dbconf = get_nntsc_db_config(nntsc_config)
        if dbconf == {}:
            sys.exit(1)

        self.db = Database(dbconf["name"], dbconf["user"], dbconf["pass"],
                dbconf["host"])

        self.exporter = exp
        self.streams = {}

        # the amp modules understand how to extract the test data from the blob
        self.amp_modules = import_data_functions()

        # set all the streams that we already know about for easy lookup of
        # their stream id when reporting data
        for i in tests:
            # force everything from the database to be proper strings rather
            # than nasty unicode so we can actually do comparisons later
            key = (str(i["modsubtype"]), str(i["source"]),
                    str(i["destination"]), str(i["packet_size"]))
            self.streams[key] = i["stream_id"]

        # Parse connection info
        username = get_nntsc_config(nntsc_config, "amp", "username")
        if username == "NNTSCConfigError":
            print >> sys.stderr, "invalid username: %s" % username
            sys.exit(1)
        password = get_nntsc_config(nntsc_config, "amp", "password")
        if password == "NNTSCConfigError":
            print >> sys.stderr, "invalid password: %s" % password
            sys.exit(1)
        host = get_nntsc_config(nntsc_config, "amp", "host")
        if host == "NNTSCConfigError":
            print >> sys.stderr, "invalid host: %s" % host
            sys.exit(1)
        port = get_nntsc_config(nntsc_config, "amp", "port")
        if port == "NNTSCConfigError":
            print >> sys.stderr, "invalid port: %s" % port
            sys.exit(1)
        ssl = get_nntsc_config_bool(nntsc_config, "amp", "ssl")
        if ssl == "NNTSCConfigError":
            print >> sys.stderr, "invalid ssl: %s" % ssl
            sys.exit(1)
        queue = get_nntsc_config(nntsc_config, "amp", "queue")
        if queue == "NNTSCConfigError":
            print >> sys.stderr, "invalid queue: %s" % queue
            sys.exit(1)

        # Connect to rabbitmq
        credentials = pika.PlainCredentials(username, password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(
                    host=host,
                    port=int(port),
                    ssl=ssl,
                    credentials=credentials)
                )
        self.channel = connection.channel()
        self.channel.queue_declare(queue=queue, durable=True)
        # limit to only one outstanding message at a time
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self.process_data, queue=queue)

    def process_data(self, channel, method, properties, body):
        """ Process a single message from the queue.
            Depending on the test this message may include multiple results.
        """
        test = properties.headers["x-amp-test-type"]
        if test in self.amp_modules:
            data = self.amp_modules[test].get_data(body)
            for d in data:
                d["source"] = properties.headers["x-amp-source-monitor"]
                d["test"] = properties.headers["x-amp-test-type"]
                # massage packetsize into appropriate format for stream key
                if d["random"]:
                    sizestr = "random"
                else:
                    sizestr = str(d["packet_size"])

                # find existing stream if it exists
                stream_id = self.find_stream(d["test"], d["source"],
                    d["target"], sizestr)

                # otherwise create a new one
                if stream_id == -1:
                    stream_id = self.add_new_stream(d["test"], d["source"],
                            d["target"], sizestr)
                if stream_id == -1:
                    print >> sys.stderr, "AMPModule: Cannot create stream for:"
                    print >> sys.stderr, "AMPModule: %s %s:%s:%s\n" % (
                            d["test"], d["source"], d["target"],
                            str(d["packet_size"]))
                    return -1

                amp_insert_data(self.db, d["test"], stream_id,
                        properties.timestamp, d)
        else:
            print >> sys.stderr, "unknown test: '%s'" % (
                    properties.headers["x-amp-test-type"])
        # TODO check if it all worked, don't ack if it fails
        self.db.commit_transaction()
        channel.basic_ack(delivery_tag = method.delivery_tag)

    def run(self):
        """ Run forever, calling the process_data callback for each message """
        print "Running amp modules: %s" % " ".join(self.amp_modules)
        self.channel.start_consuming()

    def find_stream(self, test, src, dst, size):
        """ Find the id for a stream """
        key = (test, src, dst, size)
        if key in self.streams:
            return self.streams[key]
        return -1

    def add_new_stream(self, test, src, dst, size):
        """ Add a new stream to the database """
        key = (test, src, dst, size)
        namestr = "%s %s:%s:%s" % (test, src, dst, size)
        stream_id = icmp_insert_stream(self.db, src, dst, size, namestr)
        self.streams[key] = stream_id
        return stream_id

def amp_insert_data(db, test, stream, ts, result):
    """ Insert a new datapoint into the database """
    db.insert_data(mod="amp", modsubtype=test, stream_id=stream,
            timestamp=ts, **result)

# XXX if the config file is not being used to pre-generate all the AMP streams
# then this function may no longer be required?
def insert_amp_streams(db, conf):
    pass

def run_module(tests, config, exp):
    amp = AmpModule(tests, config, exp)
    amp.run()

def tables():
    res = {}
    res["amp_icmp"] = (
            icmp_stream_table(), icmp_stream_constraints(), icmp_data_table())

    return res


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
