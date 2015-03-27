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


import sys

from libnntsc.database import DBInsert
from libnntsc.configurator import *
from libnntsc.pikaqueue import PikaConsumer, initExportPublisher, \
        PikaNNTSCException, PIKA_CONSUMER_HALT, PIKA_CONSUMER_RETRY
import pika
from ampsave.importer import import_data_functions
from ampsave.exceptions import AmpTestVersionMismatch
from libnntsc.parsers.amp_icmp import AmpIcmpParser
from libnntsc.parsers.amp_traceroute import AmpTracerouteParser
from libnntsc.parsers.amp_dns import AmpDnsParser
from libnntsc.parsers.amp_http import AmpHttpParser
from libnntsc.parsers.amp_throughput import AmpThroughputParser
from libnntsc.parsers.amp_tcpping import AmpTcppingParser
from libnntsc.dberrorcodes import *
import time
import logging

import libnntscclient.logger as logger

DEFAULT_COMMIT_FREQ=50

class AmpModule:
    def __init__(self, tests, nntsc_config, routekey, exchange, queueid):

        self.processed = 0

        logging.basicConfig()
        self.dbconf = get_nntsc_db_config(nntsc_config)
        if self.dbconf == {}:
            sys.exit(1)
        
        self.db = DBInsert(self.dbconf["name"], self.dbconf["user"], 
                self.dbconf["pass"], self.dbconf["host"])

        self.db.connect_db(15)

        # the amp modules understand how to extract the test data from the blob
        self.amp_modules = import_data_functions()

        self.collections = {}
        try:
            cols = self.db.list_collections()
        except DBQueryException as e:
            log(e)
            cols = []

        for c in cols:
            if c['module'] == "amp":
                self.collections[c['modsubtype']] = c['id']

        self.parsers = {
            "icmp":AmpIcmpParser(self.db),
            "traceroute":AmpTracerouteParser(self.db),
            "throughput":AmpThroughputParser(self.db),
            "dns":AmpDnsParser(self.db),
            "http":AmpHttpParser(self.db),
            "tcpping":AmpTcppingParser(self.db)
        }

        # set all the streams that we already know about for easy lookup of
        # their stream id when reporting data
        for i in tests:

            testtype = i["modsubtype"]
            if testtype in self.parsers:
                key = self.parsers[testtype].create_existing_stream(i)

        self.initSource(nntsc_config)
        self.exporter, self.pubthread = \
                initExportPublisher(nntsc_config, routekey, exchange, queueid)

        for k, parser in self.parsers.iteritems():
            parser.add_exporter(self.exporter)

    def initSource(self, nntsc_config):
        # Parse connection info
        username = get_nntsc_config(nntsc_config, "amp", "username")
        if username == "NNTSCConfigMissing":
            username = "amp"
        password = get_nntsc_config(nntsc_config, "amp", "password")
        if password == "NNTSCConfigMissing":
            logger.log("Password not set for AMP RabbitMQ source, using empty string as default")
            password = ""
        host = get_nntsc_config(nntsc_config, "amp", "host")
        if host == "NNTSCConfigMissing":
            host = "localhost"
        port = get_nntsc_config(nntsc_config, "amp", "port")
        if port == "NNTSCConfigMissing":
            port = "5672"
        ssl = get_nntsc_config_bool(nntsc_config, "amp", "ssl")
        if ssl == "NNTSCConfigMissing":
            ssl = False
        queue = get_nntsc_config(nntsc_config, "amp", "queue")
        if queue == "NNTSCConfigMissing":
            queue = "amp-nntsc"
        
        self.commitfreq = get_nntsc_config(nntsc_config, "amp", "commitfreq")
        if self.commitfreq == "NNTSCConfigMissing":
            self.commitfreq = DEFAULT_COMMIT_FREQ
        else:
            self.commitfreq = int(self.commitfreq)

        if "NNTSCConfigError" in [username, password, host, port, ssl, queue]:
            logger.log("Failed to configure AMP source")
            sys.exit(1)

        logger.log("Connecting to RabbitMQ queue %s on host %s:%s (ssl=%s), username %s" % (queue, host, port, ssl, username))

        self.source = PikaConsumer('', queue, host, port, 
                ssl, username, password)


    def process_data(self, channel, method, properties, body):
        """ Process a single message from the queue.
            Depending on the test this message may include multiple results.
        """

        # We need this loop so that we can try processing the message again
        # if an insert fails due to a query timeout. If
        # we exit this function without acknowledging the message then we
        # will stop getting messages (including the unacked one!)
        while 1:
            if not hasattr(properties, "user_id"):
                # ignore any messages that don't have user_id set
                channel.basic_ack(delivery_tag = method.delivery_tag)
                break
                
            test = properties.headers["x-amp-test-type"]

            if test not in self.amp_modules:
                logger.log("unknown test: '%s'" % (
                        properties.headers["x-amp-test-type"]))
                logger.log("AMP -- Data error, acknowledging and moving on")
                channel.basic_ack(delivery_tag = method.delivery_tag)
                break
               
            if test not in self.parsers:
                channel.basic_ack(delivery_tag=method.delivery_tag)
                break

            try:
                data = self.amp_modules[test].get_data(body)
            except AmpTestVersionMismatch as e:
                logger.log("Ignoring AMP result for %s test (Version mismatch): %s" % (test, e))
                data = None

            if data is None:
                channel.basic_ack(delivery_tag = method.delivery_tag)
                break

            source = properties.user_id

            try:
                self.parsers[test].process_data(properties.timestamp, data, 
                        source)
                self.processed += 1
                if self.processed >= self.commitfreq:
                    self.db.commit_data()
                    channel.basic_ack(method.delivery_tag, True)
                    self.processed = 0
                
                #if test in self.collections:
                #    self.exporter.publishPush(self.collections[test], \
                #            properties.timestamp)
            except DBQueryException as e:
                if e.code == DB_OPERATIONAL_ERROR:
                    # Disconnect while inserting data, need to reprocess the
                    # entire set of messages
                    logger.log("Database disconnect while processing AMP data")
                    channel.close()

                elif e.code == DB_DATA_ERROR:
                    # Data was bad so we couldn't insert into the database.
                    # Acknowledge the message so we can dump it from the queue
                    # and move on but don't try to export it to clients.
                    logger.log("AMP -- Data error, acknowledging and moving on")
                    channel.basic_ack(delivery_tag = method.delivery_tag)
                    break

                elif e.code == DB_INTERRUPTED:
                    logger.log("Interrupt while processing AMP data")
                    channel.close()
            
                elif e.code == DB_GENERIC_ERROR:
                    logger.log("Database error while processing AMP data")
                    channel.close()
                elif e.code == DB_QUERY_TIMEOUT:
                    logger.log("Database timeout while processing AMP data")
                    channel.close()
                elif e.code == DB_CODING_ERROR:
                    logger.log("Bad database code encountered while processing AMP data")
                    channel.close()
                elif e.code == DB_DUPLICATE_KEY:
                    logger.log("Duplicate key error while processing AMP data")
                    channel.close()
        
                else:
                    logger.log("Unknown error code returned by database: %d" \
                            % (code))
                    logger.log("Shutting down AMP module")
                    channel.close()
            break

    def run(self):
        """ Run forever, calling the process_data callback for each message """

        logger.log("Running amp modules: %s" % " ".join(self.amp_modules))


        try:
            self.source.configure([], self.process_data, self.commitfreq)
            self.source.run()
        except KeyboardInterrupt:
            self.source.halt_consumer()
        except:
            logger.log("AMP: Unknown exception during consumer loop")
            raise

        logger.log("AMP: Closed connection to RabbitMQ")

def run_module(tests, config, key, exchange, queueid):
    amp = AmpModule(tests, config, key, exchange, queueid)
    amp.run()

    amp.pubthread.join()

def tables(db):

    parser = AmpIcmpParser(db)
    parser.register()
        
    parser = AmpTracerouteParser(db)
    parser.register()

    parser = AmpDnsParser(db)
    parser.register()

    parser = AmpThroughputParser(db)
    parser.register()
    
    parser = AmpTcppingParser(db)
    parser.register()
    
    parser = AmpHttpParser(db)
    parser.register()

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
