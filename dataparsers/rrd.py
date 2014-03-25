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
        String, MetaData, ForeignKey, UniqueConstraint
from sqlalchemy.types import Integer, String, Float
from libnntsc.database import Database, DB_NO_ERROR, DB_DATA_ERROR, \
        DB_GENERIC_ERROR, DB_INTERRUPTED, DB_OPERATIONAL_ERROR
from libnntsc.configurator import *
from libnntsc.parsers import rrd_smokeping, rrd_muninbytes
import libnntscclient.logger as logger
import sys, rrdtool, socket, time
from libnntsc.pikaqueue import initExportPublisher

class RRDModule:
    def __init__(self, rrds, nntsc_conf, expqueue, exchange):

        dbconf = get_nntsc_db_config(nntsc_conf)
        if dbconf == {}:
            sys.exit(1)

        self.db = Database(dbconf["name"], dbconf["user"], dbconf["pass"],
                dbconf["host"])
        self.db.connect_db()

        self.smokepings = {}
        self.muninbytes = {}
        self.rrds = {}
        for r in rrds:
            if r['modsubtype'] == 'smokeping':
                self.smokepings[r['stream_id']] = r
            if r['modsubtype'] == 'muninbytes':
                self.muninbytes[r['stream_id']] = r

            filename = str(r['filename'])
            if filename in self.rrds:
                self.rrds[filename].append(r)
            else:
                self.rrds[filename] = [r]

        self.exporter = initExportPublisher(nntsc_conf, expqueue, exchange)

    def rejig_ts(self, endts, r):
        # Doing dumbass stuff that I shouldn't have to do to ensure
        # that the last line of output from fetch isn't full of NaNs.
        # First we try to make sure endts falls on a period boundary,
        # which you think would be enough, but even being on the
        # boundary is enough to make rrdfetch think it needs to give
        # you an extra period's worth of output, even if that output
        # is totally useless :(
        #
        # XXX Surely there must be a better way of dealing with this!

        if (endts % r['minres']) != 0:
            endts -= (endts % r['minres'])
            #endts -= 1

        startts = endts - (r['highrows'] * r['minres'])

        if (r["lasttimestamp"] > startts):
            startts = r["lasttimestamp"]

        # XXX Occasionally we manage to push our endts back past our last
        # timestamp, so we need to make sure we don't query for a broken
        # time period. This is a bit of a hax fix, but is better than nothing
        if endts < startts:
            endts = startts

        return startts, endts

    def run(self):
        logger.log("Starting RRD module")
        while True:
            for fname,rrds in self.rrds.items():
                for r in rrds:
                    stream_id = r['stream_id']
                    timestamp = int(time.mktime(time.localtime()))
                    endts = rrdtool.last(str(fname))

                    startts, endts = self.rejig_ts(endts, r)

                    fetchres = rrdtool.fetch(fname, "AVERAGE", "-s",
                            str(startts), "-e", str(endts))


                    current = int(fetchres[0][0])
                    last = int(fetchres[0][1])
                    step = int(fetchres[0][2])

                    data = fetchres[2]
                    current += step

                    for line in data:

                        if current == last:
                            break

                        code = DB_DATA_ERROR
                        if r['modsubtype'] == "smokeping":
                            code = rrd_smokeping.insert_data(self.db, 
                                    self.exporter, r['stream_id'], current, 
                                    line)

                        if r['modsubtype'] == "muninbytes":
                            code = rrd_muninbytes.insert_data(self.db, 
                                    self.exporter, r['stream_id'], current, 
                                    line)

                        if code == DB_NO_ERROR:
                            if current > r['lasttimestamp']:
                                r['lasttimestamp'] = current

                            # RRD streams are created before we see any data 
                            # so we have to update firsttimestamp when we see 
                            # the first data point
                            if (r['firsttimestamp'] == 0):
                                r['firsttimestamp'] = current;
                                code = self.db.set_firsttimestamp(
                                            r['stream_id'], 
                                            current)

                        if code == DB_GENERIC_ERROR:
                            logger.log("Database error while inserting RRD data")
                            return

                        if code == DB_DATA_ERROR:
                            logger.log("Bad RRD Data, skipping row")

                        if code == DB_INTERRUPTED:
                            logger.log("Interrupt in RRD module")
                            return

                        current += step

                    code = self.db.update_timestamp([r['stream_id']],
                            r['lasttimestamp'])

                    if code == DB_GENERIC_ERROR:
                        logger.log("Database error while updating RRD stream")
                        return
                    if code == DB_DATA_ERROR:
                        logger.log("Bad Update for RRD Data, skipping update")

                    if code == DB_INTERRUPTED:
                        logger.log("RRDModule: Interrupt in RRD module")
                        return

            self.db.commit_transaction()

            time.sleep(30)

def create_rrd_stream(db, rrdtype, params, index, existing):


    if "file" not in params:
        logger.log("Failed to create stream for RRD %d" % (index))
        logger.log("All RRDs must have a 'file' parameter")
        return

    if params['file'] in existing:
        return

    info = rrdtool.info(params['file'])
    minres = info['step']
    rows = info['rra[0].rows']
    logger.log("Creating stream for RRD-%s: %s" % (rrdtype, params['file']))

    code = DB_NO_ERROR

    if rrdtype == "smokeping":
        if "source" not in params:
            logger.log("Failed to create stream for RRD %d" % (index))
            logger.log("All Smokeping RRDs must have a 'source' parameter")

        if "host" not in params:
            logger.log("Failed to create stream for RRD %d" % (index))
            logger.log("All Smokeping RRDs must have a 'host' parameter")
            return

        if "name" not in params:
            logger.log("Failed to create stream for RRD %d" % (index))
            logger.log("All Smokeping RRDs must have a 'name' parameter")
            return

        code = rrd_smokeping.insert_stream(db, None, params['name'], 
                params['file'],
                params["source"], params['host'], minres, rows)

    if rrdtype == "muninbytes":
        if "switch" not in params:
            logger.log("Failed to create stream for RRD %d" % (index))
            logger.log("All MuninBytes RRDs must have a 'switch' parameter")
            return
        if "interface" not in params:
            logger.log("Failed to create stream for RRD %d" % (index))
            logger.log("All MuninBytes RRDs must have a 'interface' parameter")
            return
        if "direction" not in params:
            logger.log("Failed to create stream for RRD %d" % (index))
            logger.log("All MuninBytes RRDs must have a 'direction' parameter")
            return

        if params["direction"] not in ["sent", "received"]:
            logger.log("Failed to create stream for RRD %d" % (index))
            logger.log("'direction' parameter for MuninBytes RRDs must be either 'sent' or 'received'")
            return

        if "interfacelabel" not in params:
            label = None
            namelabel = "Port " + params["interface"]
        else:
            label = params["interfacelabel"]
            namelabel = label

        muninname = params["switch"] + " >> " + namelabel + " (Bytes " + params["direction"] + ")"

        code = rrd_muninbytes.insert_stream(db, None, muninname, params['file'],
                params['switch'], params['interface'], params['direction'],
                minres, rows, label)

    return code

def insert_rrd_streams(db, conf):

    rrds = db.select_streams_by_module("rrd")

    files = {}
    for r in rrds:
        files[r['filename']] = r['name']


    if conf == "":
        return

    try:
        f = open(conf, "r")
    except IOError, e:
        logger.log("WARNING: %s does not exist - no RRD streams will be added" % (conf))
        return

    logger.log("Reading RRD list from %s" % (conf))

    index = 1
    subtype = None
    parameters = {}

    for line in f:
        if line[0] == '#':
            continue
        if line == "\n" or line == "":
            continue

        x = line.strip().split("=")
        if len(x) != 2:
            continue

        if x[0] == "type":
            if parameters != {}:
                code = create_rrd_stream(db, subtype, parameters, index, 
                       files)
                if code == DB_GENERIC_ERROR:
                    logger.log("Database error while creating RRD stream")
                    return code
                if code == DB_DATA_ERROR:
                    logger.log("Invalid RRD stream description")
                    return code
                if code == DB_INTERRUPTED:
                    logger.log("RRD stream processing interrupted")
                    return code
                    
            parameters = {}
            subtype = x[1]
            index += 1
        else:
            parameters[x[0]] = x[1]


    if parameters != {}:
        code = create_rrd_stream(db, subtype, parameters, index, files)
        if code == DB_GENERIC_ERROR:
            logger.log("Database error while creating RRD stream")
            return code
        if code == DB_DATA_ERROR:
            logger.log("Invalid RRD stream description")
            return code
        if code == DB_INTERRUPTED:
            logger.log("RRD stream processing interrupted")
            return code

    db.commit_transaction()
    f.close()
    return DB_NO_ERROR


def run_module(rrds, config, key, exchange):
    rrd = RRDModule(rrds, config, key, exchange)
    rrd.run()


def tables(db):

    st_name = rrd_smokeping.stream_table(db)
    dt_name = rrd_smokeping.data_table(db)

    db.register_collection("rrd", "smokeping", st_name, dt_name)

    st_name = rrd_muninbytes.stream_table(db)
    dt_name = rrd_muninbytes.data_table(db)

    db.register_collection("rrd", "muninbytes", st_name, dt_name)

    #res = {}
    #res["rrd_smokeping"] = (smokeping_stream_table(), smokeping_stream_constraints(), smokeping_data_table())

    #return res

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
