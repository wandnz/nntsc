import sys

from libnntsc.database import Database
from libnntsc.configurator import *
import pika
from ampsave.importer import import_data_functions
from libnntsc.parsers import amp_icmp, amp_traceroute, amp_http2, amp_udpstream

class AmpModule:
    def __init__(self, tests, nntsc_config, exp):

        dbconf = get_nntsc_db_config(nntsc_config)
        if dbconf == {}:
            sys.exit(1)

        self.db = Database(dbconf["name"], dbconf["user"], dbconf["pass"],
                dbconf["host"])

        self.exporter = exp

        # the amp modules understand how to extract the test data from the blob
        self.amp_modules = import_data_functions()

        # set all the streams that we already know about for easy lookup of
        # their stream id when reporting data
        for i in tests:
            
            testtype = i["modsubtype"]

            if testtype == "icmp":
                key = amp_icmp.create_existing_stream(i)
            
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
            source = properties.headers["x-amp-source-monitor"]
            
            if test == "icmp":
                amp_icmp.process_data(self.db, self.exporter, 
                        properties.timestamp, data, source)
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


def run_module(tests, config, exp):
    amp = AmpModule(tests, config, exp)
    amp.run()

def tables(db):

    amp_icmp.register(db)
    amp_traceroute.register(db)
    amp_http2.register(db)
    amp_udpstream.register(db)

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
