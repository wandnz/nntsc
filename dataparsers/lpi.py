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


from libnntsc.database import DBInsert
from libnntsc.configurator import *
from libnntsc.parsers import lpi_bytes, lpi_common, lpi_flows
from libnntsc.parsers import lpi_users, lpi_packets
from libnntsc.pikaqueue import initExportPublisher
from libnntsc.dberrorcodes import *
import libnntscclient.logger as logger

import time

class LPIModule:
    def __init__(self, existing, nntsc_conf, expqueue, exchange):

        self.enabled = True
        self.wait = 15
        self.current_header = {}
        self.observed_protos = {}
        self.protocol_map = {}

        dbconf = get_nntsc_db_config(nntsc_conf)
        if dbconf == {}:
            self.enabled = False
            return

        self.lpiserver = get_nntsc_config(nntsc_conf, 'lpi', 'server')
        if self.lpiserver == "NNTSCConfigError" or self.lpiserver == "NNTSCConfigMissing":
            self.enabled = False
            return

        if self.lpiserver == "":
            logger.log("No LPI Server specified, disabling module")
            self.enabled = False
            return

        self.lpiport = get_nntsc_config(nntsc_conf, 'lpi', 'port')
        if self.lpiport == "NNTSCConfigError" or self.lpiport == "NNTSCConfigMissing":
            self.enabled = False
            return
        if self.lpiport == "":
            self.lpiport = 3678

        self.db = DBInsert(dbconf["name"], dbconf["user"], dbconf["pass"],
                dbconf["host"])
        self.db.connect_db(15)

        for s in existing:

            if s['modsubtype'] == "bytes":
                lpi_bytes.create_existing_stream(s)
            if s['modsubtype'] == "flows":
                lpi_flows.create_existing_stream(s)
            if s['modsubtype'] == "packets":
                lpi_packets.create_existing_stream(s)
            if s['modsubtype'] == "users":
                lpi_users.create_existing_stream(s)

        self.exporter = initExportPublisher(nntsc_conf, expqueue, exchange)

    def process_stats(self, data):
        if data == {}:
            logger.log("LPIModule: Empty Stats Dict")
            return DB_DATA_ERROR

        if data['metric'] == "bytes":
            return lpi_bytes.process_data(self.db, self.exporter, \
                    self.protocol_map, data)

        if data['metric'] == "newflows" or data['metric'] == "peakflows":
            return lpi_flows.process_data(self.db, self.exporter, \
                    self.protocol_map, data)

        if data['metric'] == "packets":
            return lpi_packets.process_data(self.db, self.exporter, \
                    self.protocol_map, data)

        if data['metric'] == "activeusers" or data['metric'] == "observedusers":
            return lpi_users.process_data(self.db, self.exporter, \
                    self.protocol_map, data)

        return DB_NO_ERROR

    def reset_seen(self):
        assert(self.protocol_map != {})

        self.current_header = {}
        self.observed_protocols = {}
        for k in self.protocol_map.keys():
            self.observed_protocols[k] = 0

    def update_seen(self, data):

        if self.current_header == {}:
            self.current_header['user'] = data['user']
            self.current_header['id'] = data['id']
            self.current_header['freq'] = data['freq']
            self.current_header['dir'] = data['dir']
            self.current_header['metric'] = data['metric']
            self.current_header['ts'] = data['ts']
        else:
            assert(data['user'] == self.current_header['user'])
            assert(data['id'] == self.current_header['id'])
            assert(data['freq'] == self.current_header['freq'])
            assert(data['dir'] == self.current_header['dir'])
            assert(data['metric'] == self.current_header['metric'])
            assert(data['ts'] == self.current_header['ts'])

        for k in data['results'].keys():
            assert(k in self.observed_protocols)
            del self.observed_protocols[k]

    def insert_zeroes(self):
        assert(self.current_header != 0)

        data = {}
        data['user'] = self.current_header['user']
        data['id'] = self.current_header['id']
        data['freq'] = self.current_header['freq']
        data['dir'] = self.current_header['dir']
        data['ts'] = self.current_header['ts']
        data['metric'] = self.current_header['metric']
        data['results'] = {}

        for k in self.observed_protocols.keys():
            data['results'][k] = 0


        return self.process_stats(data)

    def run(self):
        while self.enabled:
            logger.log("Attempting to connect to LPI Server %s:%s" % (self.lpiserver, self.lpiport))
            self.server_fd = lpi_common.connect_lpi_server(self.lpiserver,
                    int(self.lpiport))
            if self.server_fd == -1:
                logger.log("Connection failed -- will retry in %d seconds" %
                        self.wait)
                time.sleep(self.wait)
                self.wait *= 2
                if self.wait > 600:
                    self.wait = 600
                continue

            logger.log("Successfully connected to LPI Server")
            self.wait = 15
            self.protocol_map = {}

            while True:
                rec_type, data = lpi_common.read_lpicp(self.server_fd)

                if rec_type == 3:
                    if self.insert_zeroes() == DB_NO_ERROR:
                        self.db.commit_transaction()
                    self.reset_seen()

                if rec_type == 4:
                    self.protocol_map = data
                    self.reset_seen()

                if rec_type == 0:
                    self.update_seen(data)
                    code = self.process_stats(data)
                   
                    if code == DB_INTERRUPTED:
                        logger.log("Interrupt while processing LPI data")
                        break
                        
                    if code == DB_GENERIC_ERROR:
                        logger.log("Database error while processing LPI data")
                        break
                         
                    if code == DB_DATA_ERROR:
                        # Bad data -- reconnect to server  
                        logger.log("LPIModule: Invalid Statistics Data")
                        break

                    if code == DB_CODING_ERROR:
                        logger.log("Bad database code encountered while processing LPI data -- skipping data")
                        continue

                    if code == DB_DUPLICATE_KEY:
                        logger.log("Duplicate key error while processing LPI data")
                        break

                    if code == DB_QUERY_TIMEOUT:
                        logger.log("Query timeout while inserting LPI data -- should this really be happening?")
                        break

                    assert(code != DB_OPERATIONAL_ERROR)

                if rec_type == -1:
                    break

            self.server_fd.close()


def run_module(existing, config, key, exchange):
    lpi = LPIModule(existing, config, key, exchange)
    lpi.run()

def tables(db):

    st_name = lpi_bytes.stream_table(db)
    dt_name = lpi_bytes.data_table(db)
    db.register_collection("lpi", "bytes", st_name, dt_name)

    st_name = lpi_flows.stream_table(db)
    dt_name = lpi_flows.data_table(db)
    db.register_collection("lpi", "flows", st_name, dt_name)

    st_name = lpi_packets.stream_table(db)
    dt_name = lpi_packets.data_table(db)
    db.register_collection("lpi", "packets", st_name, dt_name)

    st_name = lpi_users.stream_table(db)
    dt_name = lpi_users.data_table(db)
    db.register_collection("lpi", "users", st_name, dt_name)



# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
