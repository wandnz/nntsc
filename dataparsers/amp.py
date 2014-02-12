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

from libnntsc.database import Database, DB_NO_ERROR, DB_DATA_ERROR, \
        DB_GENERIC_ERROR
from libnntsc.configurator import *
from libnntsc.pikaqueue import PikaConsumer, initExportPublisher
import pika
from ampsave.importer import import_data_functions
from libnntsc.parsers import amp_icmp, amp_traceroute, amp_dns, amp_http
import time
import logging

import libnntscclient.logger as logger

class AmpModule:
    def __init__(self, tests, nntsc_config, expqueue, exchange):

        logging.basicConfig()
        dbconf = get_nntsc_db_config(nntsc_config)
        if dbconf == {}:
            sys.exit(1)

        self.db = Database(dbconf["name"], dbconf["user"], dbconf["pass"],
                dbconf["host"])

        self.db.connect_db()

        # the amp modules understand how to extract the test data from the blob
        self.amp_modules = import_data_functions()

        self.collections = {}
        cols = self.db.list_collections()

        for c in cols:
            if c['module'] == "amp":
                self.collections[c['modsubtype']] = c['id']

        # set all the streams that we already know about for easy lookup of
        # their stream id when reporting data
        for i in tests:

            testtype = i["modsubtype"]
            if testtype == "icmp":
                key = amp_icmp.create_existing_stream(i)
            elif testtype == "traceroute":
                key = amp_traceroute.create_existing_stream(i)
            elif testtype == "dns":
                key = amp_dns.create_existing_stream(i)
            elif testtype == "http":
                key = amp_http.create_existing_stream(i)


        self.initSource(nntsc_config)
        self.exporter = initExportPublisher(nntsc_config, expqueue, exchange)

    def initSource(self, nntsc_config):
        # Parse connection info
        username = get_nntsc_config(nntsc_config, "amp", "username")
        if username == "NNTSCConfigError":
            logger.log("Invalid username option for AMP")
            sys.exit(1)
        password = get_nntsc_config(nntsc_config, "amp", "password")
        if password == "NNTSCConfigError":
            logger.log("Invalid password option for AMP")
            sys.exit(1)
        host = get_nntsc_config(nntsc_config, "amp", "host")
        if host == "NNTSCConfigError":
            logger.log("Invalid host option for AMP")
            sys.exit(1)
        port = get_nntsc_config(nntsc_config, "amp", "port")
        if port == "NNTSCConfigError":
            logger.log("Invalid port option for AMP")
            sys.exit(1)
        ssl = get_nntsc_config_bool(nntsc_config, "amp", "ssl")
        if ssl == "NNTSCConfigError":
            logger.log("Invalid ssl option for AMP")
            sys.exit(1)
        queue = get_nntsc_config(nntsc_config, "amp", "queue")
        if queue == "NNTSCConfigError":
            logger.log("Invalid queue option for AMP")
            sys.exit(1)

        self.source = PikaConsumer('', queue, host, port, 
                ssl, username, password)


    def process_data(self, channel, method, properties, body):
        """ Process a single message from the queue.
            Depending on the test this message may include multiple results.
        """

        # We need this loop so that we can try processing the message again
        # if a DB_GENERIC_ERROR requires us to reconnect to the database. If
        # we exit this function without acknowledging the message then we
        # will stop getting messages (including the unacked one!)
        while 1:
            if hasattr(properties, "user_id"):
                test = properties.headers["x-amp-test-type"]
                if test in self.amp_modules:
                    data = self.amp_modules[test].get_data(body)
                    source = properties.user_id
                    if test == "icmp":
                        code = amp_icmp.process_data(self.db, self.exporter,
                                properties.timestamp, data, source)
                    elif test == "traceroute":
                        code = amp_traceroute.process_data(self.db, 
                                self.exporter, properties.timestamp, data, 
                                source)
                    elif test == "dns":
                        code = amp_dns.process_data(self.db, self.exporter,
                                properties.timestamp, data, source)
                    elif test == "http":
                        code = amp_http.process_data(self.db, self.exporter,
                                properties.timestamp, data, source)
                    else:
                        code = DB_DATA_ERROR
                else:
                    logger.log("unknown test: '%s'" % (
                            properties.headers["x-amp-test-type"]))
                    code = DB_DATA_ERROR

                if code == DB_NO_ERROR:
                    self.db.commit_transaction()

                    if test in self.collections:
                        self.exporter.publishPush(self.collections[test], \
                                properties.timestamp)
                    channel.basic_ack(delivery_tag = method.delivery_tag)
                    break

                elif code == DB_DATA_ERROR:
                    # Data was bad so we couldn't insert into the database.
                    # Acknowledge the message so we can dump it from the queue
                    # and move on but don't try to export it to clients.
                    logger.log("AMP -- Data error, acknowledging and moving on")
                    channel.basic_ack(delivery_tag = method.delivery_tag)
                    break

                else:
                    # Some other error cropped up. Best approach here is to 
                    # reconnect to the database and try to process the message
                    # again. Unfortunately SQLAlchemy gives us no indication
                    # of whether the connect was successful until we try to
                    # operate on the connection, so we'll just have to loop
                    # around and try process the message again
                    logger.log("AMP -- reconnecting to database after error")
                    time.sleep(5)
                    self.db.connect_db()
                    logger.log("AMP -- reconnected")

            else:
                # ignore any messages that don't have user_id set
                channel.basic_ack(delivery_tag = method.delivery_tag)
                break

    def run(self):
        """ Run forever, calling the process_data callback for each message """

        logger.log("Running amp modules: %s" % " ".join(self.amp_modules))
        self.source.connect()
        self.source.configure_consumer(self.process_data)

        self.source.run_consumer()
        logger.log("AMP: Closed connection to RabbitMQ")

def run_module(tests, config, key, exchange):
    amp = AmpModule(tests, config, key, exchange)
    amp.run()

def tables(db):

    amp_icmp.register(db)
    amp_traceroute.register(db)
    amp_dns.register(db)
    amp_http.register(db)
    #amp_udpstream.register(db)

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
