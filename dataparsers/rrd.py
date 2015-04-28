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
from libnntsc.parsers.rrd_smokeping import RRDSmokepingParser
from libnntsc.parsers.rrd_muninbytes import RRDMuninbytesParser
import libnntscclient.logger as logger
import sys, rrdtool, socket, time, signal
from libnntsc.pikaqueue import initExportPublisher

RRD_RETRY = 0
RRD_CONTINUE = 1
RRD_HALT = 2

class RRDModule:
    def __init__(self, rrds, nntsc_conf, routekey, exchange, queueid):

        self.exporter = None
        self.pubthread = None

        dbconf = get_nntsc_db_config(nntsc_conf)
        if dbconf == {}:
            sys.exit(1)

    
        self.db = DBInsert(dbconf["name"], dbconf["user"], dbconf["pass"],
                dbconf["host"], cachetime=dbconf['cachetime'])
        self.db.connect_db(15)

        self.smokeparser = RRDSmokepingParser(self.db)
        self.muninparser = RRDMuninbytesParser(self.db)

        self.smokepings = {}
        self.muninbytes = {}
        self.rrds = {}
        for r in rrds:
            if r['modsubtype'] == 'smokeping':
                lastts = self.smokeparser.get_last_timestamp(r['stream_id'])
                r['lasttimestamp'] = lastts
                self.smokepings[r['stream_id']] = r
            elif r['modsubtype'] == 'muninbytes':
                lastts = self.muninparser.get_last_timestamp(r['stream_id'])
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

        liveconf = get_nntsc_config_bool(nntsc_config, "liveexport", "enabled")
        if liveconf == "NNTSCConfigError":
            logger.log("Bad 'enabled' option for liveexport -- disabling")
            liveconf = False

        if liveconf == "NNTSCConfigMissing":
            liveconf = True

        if liveconf:
            self.exporter, self.pubthread = \
                    initExportPublisher(nntsc_conf, routekey, exchange, queueid)

            self.smokeparser.add_exporter(self.exporter)
            self.muninparser.add_exporter(self.exporter)

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

        if startts == endts:
            return

        fetchres = rrdtool.fetch(fname, "AVERAGE", "-s",
                str(startts), "-e", str(endts))

        current = int(fetchres[0][0])
        last = int(fetchres[0][1])
        step = int(fetchres[0][2])

        data = fetchres[2]
        current += step

        update_needed = False
        datatable = None

        for line in data:

            if current == last:
                break

            code = DB_DATA_ERROR
            if r['modsubtype'] == "smokeping":
                parser = self.smokeparser
            elif r['modsubtype'] == "muninbytes":
                parser = self.muninparser
            else:
                break
                
            parser.process_data(r['stream_id'], current, line)
            if datatable is None:
                datatable = parser.get_data_table_name()
            
            if current > r['lasttimestamp']:
                r['lasttimestamp'] = current
                update_needed = True
                
            current += step

        if not update_needed:
            return
        
        self.db.commit_data()

        if datatable is not None:
            self.db.update_timestamp(datatable, [r['stream_id']],
                r['lasttimestamp'])

    def rrdloop(self):
        for fname,rrds in self.rrds.items():
            for r in rrds:
                try:
                    self.read_from_rrd(r, fname)
                except DBQueryException as e:
                    if e.code == DB_QUERY_TIMEOUT:
                        return RRD_RETRY
                    if e.code == DB_OPERATIONAL_ERROR:
                        return RRD_RETRY
                    if e.code == DB_INTERRUPTED:
                        return RRD_HALT
                    # Ignore other DB errors as they represent bad data or 
                    # database code. Try to carry on to the next RRD instead.
                    continue
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

            time.sleep(30)

        logger.log("Halting RRD module")

    def revert_rrds(self):
        logger.log("Reverting RRD timestamps to previous safe value")
        for fname,rrds in self.rrds.items():
            for r in rrds:
                if 'lastcommit' in r:
                    r['lasttimestamp'] = r['lastcommit']

def create_rrd_stream(db, rrdtype, params, index, existing, parser):

    if parser is None:
        return DB_NO_ERROR

    if "file" not in params:
        logger.log("Failed to create stream for RRD %d" % (index))
        logger.log("All RRDs must have a 'file' parameter")
        return DB_DATA_ERROR

    if params['file'] in existing:
        return DB_NO_ERROR

    info = rrdtool.info(params['file'])
    params['minres'] = info['step']
    params['highrows'] = info['rra[0].rows']
    logger.log("Creating stream for RRD-%s: %s" % (rrdtype, params['file']))

    parser.insert_stream(params)

def insert_rrd_streams(db, conf):
    smoke = RRDSmokepingParser(db)
    munin = RRDMuninbytesParser(db)

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
                if subtype == "smokeping":
                    parser = smoke
                elif subtype == "muninbytes":
                    parser = munin
                else:
                    parser = None
                
                try: 
                    create_rrd_stream(db, subtype, parameters, index, 
                       files, parser)
                except DBQueryException as e:
                    logger.log("Exception while creating RRD streams, aborting")
                    return
                    
            parameters = {}
            subtype = x[1]
            index += 1
        else:
            parameters[x[0]] = x[1]


    if parameters != {}:
        if subtype == "smokeping":
            parser = smoke
        elif subtype == "muninbytes":
            parser = munin
        else:
            parser = None
        
        try: 
            create_rrd_stream(db, subtype, parameters, index, 
               files, parser)
        except DBQueryException as e:
            logger.log("Exception while creating RRD streams, aborting")
            return

    f.close()


def run_module(rrds, config, key, exchange, queueid):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    rrd = RRDModule(rrds, config, key, exchange, queueid)
    rrd.run()

    if rrd.pubthread:
        rrd.pubthread.join()

def tables(db):

    smoke = RRDSmokepingParser(db)
    munin = RRDMuninbytesParser(db)

    smoke.register()
    munin.register()


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
