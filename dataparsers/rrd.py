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
from libnntsc.dberrorcodes import *
from libnntsc.configurator import *
from libnntsc.parsers import rrd_smokeping, rrd_muninbytes
import libnntscclient.logger as logger
import sys, rrdtool, socket, time
from libnntsc.pikaqueue import initExportPublisher

RRD_RETRY = 0
RRD_CONTINUE = 1
RRD_HALT = 2

class RRDModule:
    def __init__(self, rrds, nntsc_conf, expqueue, exchange):

        dbconf = get_nntsc_db_config(nntsc_conf)
        if dbconf == {}:
            sys.exit(1)

    
        self.db = DBInsert(dbconf["name"], dbconf["user"], dbconf["pass"],
                dbconf["host"], cachetime=dbconf['cachetime'])
        self.db.connect_db(15)

        self.smokepings = {}
        self.muninbytes = {}
        self.rrds = {}
        for r in rrds:
            if r['modsubtype'] == 'smokeping':
                lastts = rrd_smokeping.get_last_timestamp(self.db, 
                        r['stream_id'])
                r['lasttimestamp'] = lastts
                self.smokepings[r['stream_id']] = r
            elif r['modsubtype'] == 'muninbytes':
                lastts = rrd_muninbytes.get_last_timestamp(self.db, 
                        r['stream_id'])
                r['lasttimestamp'] = lastts
                self.muninbytes[r['stream_id']] = r
            else:
                continue

            r['lastcommit'] = r['lasttimestamp']
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

    def read_from_rrd(self, r, fname):
        r['lastcommit'] = r['lasttimestamp']
        stream_id = r['stream_id']
        endts = rrdtool.last(str(fname))

        startts, endts = self.rejig_ts(endts, r)

        fetchres = rrdtool.fetch(fname, "AVERAGE", "-s",
                str(startts), "-e", str(endts))

        current = int(fetchres[0][0])
        last = int(fetchres[0][1])
        step = int(fetchres[0][2])

        data = fetchres[2]
        current += step

        update_needed = False

        for line in data:

            if current == last:
                break

            code = DB_DATA_ERROR
            if r['modsubtype'] == "smokeping":
                code = rrd_smokeping.process_data(self.db, 
                        self.exporter, r['stream_id'], current, 
                        line)

            if r['modsubtype'] == "muninbytes":
                code = rrd_muninbytes.process_data(self.db, 
                        self.exporter, r['stream_id'], current, 
                        line)

            if code == DB_NO_ERROR:
                if current > r['lasttimestamp']:
                    r['lasttimestamp'] = current
                    update_needed = True

            if code == DB_QUERY_TIMEOUT or code == DB_OPERATIONAL_ERROR:
                return code
                
            if code == DB_INTERRUPTED:
                logger.log("Interrupt in RRD module")
                return code

            if code != DB_NO_ERROR:
                logger.log("Error while inserting RRD data")

            current += step

        if not update_needed:
            return DB_NO_ERROR

        code = self.db.update_timestamp([r['stream_id']],
                r['lasttimestamp'])

        if code == DB_QUERY_TIMEOUT or code == DB_OPERATIONAL_ERROR:
            return code
        if code == DB_INTERRUPTED:
            logger.log("Interrupt in RRD module")
            return code

        if code != DB_NO_ERROR:
            logger.log("Error while updating last timestamp for RRD stream")
            return code

        return DB_NO_ERROR

    def rrdloop(self):
        for fname,rrds in self.rrds.items():
            for r in rrds:
                err = self.read_from_rrd(r, fname)

                if err == DB_QUERY_TIMEOUT or err == DB_OPERATIONAL_ERROR:
                    return RRD_RETRY
                if err == DB_INTERRUPTED:
                    return RRD_HALT
                # Ignore other DB errors as they represent bad data or 
                # database code. Try to carry on to the next RRD instead.
        return RRD_CONTINUE


    def run(self):
        logger.log("Starting RRD module")
        while True:
            result = self.rrdloop()
            
            if result == RRD_RETRY:
                self.revert_rrds()
                time.sleep(10)
                continue

            if result == RRD_HALT:
                break

            err = self.db.commit_data()
            if err == DB_QUERY_TIMEOUT or err == DB_OPERATIONAL_ERROR:
                # Revert our lasttimestamp back to whenever we last
                # successfully committed to ensure we re-insert the data
                self.revert_rrds()
                time.sleep(10)
                continue

            if err != DB_NO_ERROR:
                logger.log("Error while committing RRD Data")
                self.revert_rrds()

            time.sleep(30)

        logger.log("Halting RRD module")

    def revert_rrds(self):
        logger.log("Reverting RRD timestamps to previous safe value")
        for fname,rrds in self.rrds.items():
            for r in rrds:
                if 'lastcommit' in r:
                    r['lasttimestamp'] = r['lastcommit']

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

        code = rrd_smokeping.insert_stream(db, None,  
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

        code = rrd_muninbytes.insert_stream(db, None, params['file'],
                params['switch'], params['interface'], params['direction'],
                minres, rows, label)

    return code

def insert_rrd_streams(db, conf):

    try:
        rrds = db.select_streams_by_module("rrd")
    except DBQueryException as e:
        logger.log("Error while fetching existing RRD streams from database")
        return

    files = set()
    for r in rrds:
        files.add(r['filename'])


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
                if code == DB_DUPLICATE_KEY:
                    # Note, we should not see this error as we should get
                    # back the id of the duplicate stream instead
                    logger.log("Duplicate key error while inserting RRD stream")
                    return code
                if code == DB_CODING_ERROR:
                    logger.log("Programming error while inserting RRD stream")
                    return code
                if code == DB_QUERY_TIMEOUT:
                    logger.log("Timeout while inserting RRD stream")
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
        if code == DB_DUPLICATE_KEY:
            # Note, we should not see this error as we should get
            # back the id of the duplicate stream instead
            logger.log("Duplicate key error while inserting RRD stream")
            return code
        if code == DB_CODING_ERROR:
            logger.log("Programming error while inserting RRD stream")
            return code
        if code == DB_QUERY_TIMEOUT:
            logger.log("Timeout while inserting RRD stream")
            return code

    f.close()
    return DB_NO_ERROR


def run_module(rrds, config, key, exchange):
    rrd = RRDModule(rrds, config, key, exchange)
    rrd.run()


def tables(db):

    st_name = rrd_smokeping.stream_table(db)
    dt_name = rrd_smokeping.data_table(db)

    if st_name == None or dt_name == None:
        logger.log("Error creating RRD Smokeping base tables")
        return

    code = db.register_collection("rrd", "smokeping", st_name, dt_name)

    if code != DB_NO_ERROR and code != DB_DUPLICATE_KEY:
        logger.log("Failed to register rrd smokeping collection")
        return

    st_name = rrd_muninbytes.stream_table(db)
    dt_name = rrd_muninbytes.data_table(db)

    if st_name == None or dt_name == None:
        logger.log("Error creating RRD Muninbytes base tables")
        return
    db.register_collection("rrd", "muninbytes", st_name, dt_name)

    if code != DB_NO_ERROR and code != DB_DUPLICATE_KEY:
        logger.log("Failed to register rrd muninbytes collection")
        return
    #res = {}
    #res["rrd_smokeping"] = (smokeping_stream_table(), smokeping_stream_constraints(), smokeping_data_table())

    #return res

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
