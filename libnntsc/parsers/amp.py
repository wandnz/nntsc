#
# This file is part of NNTSC.
#
# Copyright (C) 2013-2017 The University of Waikato, Hamilton, New Zealand.
#
# Authors: Shane Alcock
#          Brendon Jones
#
# All rights reserved.
#
# This code has been developed by the WAND Network Research Group at the
# University of Waikato. For further information please see
# http://www.wand.net.nz/
#
# NNTSC is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 as
# published by the Free Software Foundation.
#
# NNTSC is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with NNTSC; if not, write to the Free Software Foundation, Inc.
# 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#
# Please report any bugs, questions or comments to contact@wand.net.nz
#


import sys
import signal
import logging

import ampsave
from libnntsc.database import DBInsert
from libnntsc.influx import InfluxInsertor
from libnntsc.configurator import *
from libnntsc.pikaqueue import PikaConsumer, initExportPublisher
from libnntsc.parsers.amp_icmp import AmpIcmpParser
from libnntsc.parsers.amp_traceroute import AmpTracerouteParser
from libnntsc.parsers.amp_traceroute_pathlen import AmpTraceroutePathlenParser
from libnntsc.parsers.amp_dns import AmpDnsParser
from libnntsc.parsers.amp_http import AmpHttpParser
from libnntsc.parsers.amp_throughput import AmpThroughputParser
from libnntsc.parsers.amp_tcpping import AmpTcppingParser
from libnntsc.parsers.amp_udpstream import AmpUdpstreamParser
from libnntsc.parsers.amp_youtube import AmpYoutubeParser
from libnntsc.parsers.amp_fastping import AmpFastpingParser
from libnntsc.parsers.amp_external import AmpExternalParser
from libnntsc.parsers.amp_sip import AmpSipParser
from libnntsc.dberrorcodes import *
from google.protobuf.message import DecodeError

import libnntscclient.logger as logger

DEFAULT_COMMIT_FREQ = 50

class AmpModule:
    def __init__(self, tests, nntsc_config, routekey, exchange, queueid):

        self.pending = []
        self.exporter = None
        self.pubthread = None

        logging.basicConfig()
        self.dbconf = get_nntsc_db_config(nntsc_config)
        if self.dbconf == {}:
            sys.exit(1)

        self.db = DBInsert(self.dbconf["name"], self.dbconf["user"],
                self.dbconf["pass"], self.dbconf["host"])

        self.db.connect_db(15)

        self.influxconf = get_influx_config(nntsc_config)
        if self.influxconf == {}:
            sys.exit(1)

        if self.influxconf["useinflux"]:
            self.influxdb = InfluxInsertor(
                self.influxconf["name"], self.influxconf["user"], self.influxconf["pass"],
                self.influxconf["host"], self.influxconf["port"])
        else:
            self.influxdb = None

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
            "icmp": [AmpIcmpParser(self.db, self.influxdb)],
            "traceroute": [AmpTracerouteParser(self.db),
                    AmpTraceroutePathlenParser(self.db, self.influxdb)
                    ],
            "throughput": [AmpThroughputParser(self.db, self.influxdb)],
            "dns": [AmpDnsParser(self.db, self.influxdb)],
            "http": [AmpHttpParser(self.db, self.influxdb)],
            "udpstream": [AmpUdpstreamParser(self.db, self.influxdb)],
            "tcpping": [AmpTcppingParser(self.db, self.influxdb)],
            "youtube": [AmpYoutubeParser(self.db, self.influxdb)],
            "fastping": [AmpFastpingParser(self.db, self.influxdb)],
            "external": [AmpExternalParser(self.db, self.influxdb)],
            "sip": [AmpSipParser(self.db, self.influxdb)],
        }

        # set all the streams that we already know about for easy lookup of
        # their stream id when reporting data
        for i in tests:

            testtype = i["modsubtype"]
            if testtype in self.parsers:
                for p in self.parsers[testtype]:
                    p.create_existing_stream(i)

        self.initSource(nntsc_config)

        liveconf = get_nntsc_config_bool(nntsc_config, "liveexport", "enabled")
        if liveconf == "NNTSCConfigError":
            logger.log("Bad 'enabled' option for liveexport -- disabling")
            liveconf = False

        if liveconf == "NNTSCConfigMissing":
            liveconf = True

        if liveconf:
            self.exporter, self.pubthread = \
                    initExportPublisher(nntsc_config, routekey, exchange, \
                    queueid)

            for parsers in self.parsers.values():
                for p in parsers:
                    p.add_exporter(self.exporter)

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
                ssl, username, password, True)


    def process_data(self, channel, method, properties, body):
        """ Process a single message from the queue.
            Depending on the test this message may include multiple results.
        """

        # push every new message onto the end of the list to be processed
        self.pending.append((method, properties, body))

        # once there are enough messages ready to go, process them all
        if len(self.pending) < self.commitfreq:
            return

        # track how many messages were successfully written to the database
        processed = 0

        for method, properties, body in self.pending:
            # ignore any messages that don't have user_id set
            if not hasattr(properties, "user_id"):
                continue

            test = properties.headers["x-amp-test-type"]

            # ignore any messages for tests we don't have a parser for
            if test not in self.parsers:
                continue

            try:
                data = ampsave.get_data(test, body)
            except DecodeError as e:
                # we got something that wasn't a valid protocol buffer message
                logger.log("Failed to decode result from %s for %s test: %s" % (
                            properties.user_id, test, e))
                data = None
            except AssertionError as e:
                # A lot of ampsave functions assert fail if something goes
                # wrong, so we need to catch that and chuck the bogus data
                logger.log("Ignoring AMP result for %s test (ampsave assertion failure): %s" % (test, e))
                data = None
            except ampsave.UnknownTestError as e:
                logger.log("unknown test: '%s'" % test)
                logger.log("AMP -- Data error, acknowledging and moving on")
                data = None

            # ignore any broken messages and carry on
            if data is None:
                continue

            try:
                # pass the message off to the test specific code
                for p in self.parsers[test]:
                    p.process_data(properties.timestamp, data,
                            properties.user_id)
                processed += 1
            except DBQueryException as e:
                logger.log(e)

                if e.code == DB_DATA_ERROR:
                    # Data was bad so we couldn't insert into the database.
                    # Acknowledge the message so we can dump it from the queue
                    # and move on but don't try to export it to clients.
                    # TODO I assume this doesn't break the current transaction
                    logger.log("Acknowledging data and moving on")
                    continue

                # disconnect and restart processing in a new transaction
                channel.close()
                self.pending = []
                return

        # commit the data if anything was successfully processed
        if processed > 0:
            try:
                self.db.commit_data()
                if self.influxdb:
                    self.influxdb.commit_data()
            except DBQueryException as e:
                logger.log(e)
                channel.close()
                self.pending = []
                return

            # some parsers need to update internal caches after confirming
            # that data has been committed successfully
            # TODO limit this to parsers that recently processed data
            for parsers in list(self.parsers.values()):
                for parser in parsers:
                    parser.post_commit()

        # ack all data up to and including the most recent message
        channel.basic_ack(method.delivery_tag, True)

        # empty the list of pending data ready for more
        self.pending = []

    def run(self):
        """ Run forever, calling the process_data callback for each message """

        logger.log("Running amp modules: %s" % " ".join(ampsave.list_modules()))

        try:
            self.source.configure([], self.process_data, self.commitfreq)
            self.source.run()
        except KeyboardInterrupt:
            self.source.halt_consumer()
        except Exception as e:
            # XXX if this fires, can we do something better than going defunct?
            logger.log("AMP: Unknown exception during consumer loop")
            logger.log(e)
            raise

        logger.log("AMP: Closed connection to RabbitMQ")

def run_module(tests, config, key, exchange, queueid):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    amp = AmpModule(tests, config, key, exchange, queueid)
    amp.run()

    if amp.pubthread:
        amp.pubthread.join()

def create_cqs(db, influxdb):

    for p in [AmpIcmpParser, AmpDnsParser, AmpThroughputParser,
                AmpTcppingParser, AmpHttpParser, AmpUdpstreamParser,
                AmpTraceroutePathlenParser, AmpYoutubeParser,
                AmpFastpingParser, AmpExternalParser, AmpSipParser]:
        parser = p(db, influxdb)
        parser.build_cqs()

def tables(db):

    for p in [AmpIcmpParser, AmpDnsParser, AmpThroughputParser,
                AmpTcppingParser, AmpHttpParser, AmpUdpstreamParser,
                AmpTraceroutePathlenParser, AmpTracerouteParser,
                AmpYoutubeParser, AmpFastpingParser, AmpExternalParser,
                AmpSipParser]:
        parser = p(db)
        parser.register()
# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
