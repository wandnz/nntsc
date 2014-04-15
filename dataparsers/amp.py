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
from libnntsc.parsers import amp_icmp, amp_traceroute, amp_dns
from libnntsc.dberrorcodes import *
import time
import logging

import libnntscclient.logger as logger

COMMIT_THRESH=50

class AmpModule:
    def __init__(self, tests, nntsc_config, expqueue, exchange):

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
            #elif testtype == "http":
            #    key = amp_http.create_existing_stream(i)


        self.initSource(nntsc_config)
        self.exporter = initExportPublisher(nntsc_config, expqueue, exchange)


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
                        channel.basic_ack(delivery_tag=method.delivery_tag)
                        break
                    #    code = amp_http.process_data(self.db, self.exporter,
                    #            properties.timestamp, data, source)
                    else:
                        code = DB_DATA_ERROR
                else:
                    logger.log("unknown test: '%s'" % (
                            properties.headers["x-amp-test-type"]))
                    code = DB_DATA_ERROR

                # Inserts were successful, commit data and update error code
                if code == DB_NO_ERROR:
                    self.processed += 1
                    if self.processed >= COMMIT_THRESH:
                        code = self.db.commit_data()

                if code == DB_NO_ERROR:
                    if test in self.collections:
                        self.exporter.publishPush(self.collections[test], \
                                properties.timestamp)
                    if self.processed >= COMMIT_THRESH:
                        channel.basic_ack(method.delivery_tag, True)
                        self.processed = 0
                    break
                
                if code == DB_OPERATIONAL_ERROR:
                    # Disconnect while inserting data, need to reprocess the
                    # entire set of messages
                    logger.log("Database disconnect while processing AMP data")
                    raise PikaNNTSCException(True)

                elif code == DB_DATA_ERROR:
                    # Data was bad so we couldn't insert into the database.
                    # Acknowledge the message so we can dump it from the queue
                    # and move on but don't try to export it to clients.
                    logger.log("AMP -- Data error, acknowledging and moving on")
                    channel.basic_ack(delivery_tag = method.delivery_tag)
                    break

                elif code == DB_INTERRUPTED:
                    logger.log("Interrupt while processing AMP data")
                    raise PikaNNTSCException(False)
                
                elif code == DB_GENERIC_ERROR:
                    logger.log("Database error while processing AMP data")
                    raise PikaNNTSCException(False)
                elif code == DB_QUERY_TIMEOUT:
                    logger.log("Database timeout while processing AMP data")
                    continue
                elif code == DB_CODING_ERROR:
                    logger.log("Bad database code encountered while processing AMP data")
                    raise PikaNNTSCException(False)
                elif code == DB_DUPLICATE_KEY:
                    logger.log("Duplicate key error while processing AMP data")
                    raise PikaNNTSCException(False)
            
                else:
                    logger.log("Unknown error code returned by database: %d" % (code))
                    logger.log("Shutting down AMP module")
                    raise PikaNNTSCException(False)

            else:
                # ignore any messages that don't have user_id set
                channel.basic_ack(delivery_tag = method.delivery_tag)
                break

    def run(self):
        """ Run forever, calling the process_data callback for each message """

        logger.log("Running amp modules: %s" % " ".join(self.amp_modules))

        while 1:
            self.source.connect()
            self.source.configure_consumer(self.process_data, COMMIT_THRESH)
            
            retval = self.source.run_consumer()
            if retval == PIKA_CONSUMER_HALT:
                break

        logger.log("AMP: Closed connection to RabbitMQ")

def run_module(tests, config, key, exchange):
    amp = AmpModule(tests, config, key, exchange)
    amp.run()

def tables(db):

    code = amp_icmp.register(db)
    if code != DB_NO_ERROR and code != DB_DUPLICATE_KEY:
        logger.log("Failed to register AMP ICMP collection")

    code = amp_traceroute.register(db)
    if code != DB_NO_ERROR and code != DB_DUPLICATE_KEY:
        logger.log("Failed to register AMP Traceroute collection")
    
    code = amp_dns.register(db)
    if code != DB_NO_ERROR and code != DB_DUPLICATE_KEY:
        logger.log("Failed to register AMP DNS collection")

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
