from sqlalchemy import create_engine, Table, Column, Integer, \
        String, MetaData, ForeignKey, UniqueConstraint
from sqlalchemy.types import Integer, String, Float
from libnntsc.database import Database
from libnntsc.configurator import *
from libnntsc.parsers import rrd_smokeping, rrd_muninbytes
import sys, rrdtool, socket, time

class RRDModule:
    def __init__(self, rrds, nntsc_conf, exp):

        dbconf = get_nntsc_db_config(nntsc_conf)
        if dbconf == {}:
            sys.exit(1)

        self.db = Database(dbconf["name"], dbconf["user"], dbconf["pass"], 
                dbconf["host"])

        self.exporter = exp
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
            
        return startts, endts

    def run(self):
        print "Running rrd"
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
                        if r['modsubtype'] == "smokeping":
                            rrd_smokeping.insert_data(self.db, self.exporter,
                                    r['stream_id'], current, line)
                        
                        if r['modsubtype'] == "muninbytes":
                            rrd_muninbytes.insert_data(self.db, self.exporter,
                                    r['stream_id'], current, line)
                        
                        if current > r['lasttimestamp']:
                            r['lasttimestamp'] = current
                        current += step
                    
                    self.db.update_timestamp(r['stream_id'], r['lasttimestamp']) 
            self.db.commit_transaction()

            time.sleep(30)

def create_rrd_stream(db, rrdtype, params, index, existing):

    if "name" not in params:
        print >> sys.stderr, "Failed to create stream for RRD %d" % (index)
        print >> sys.stderr, "All RRDs must have a 'name' parameter"
        return
    
    if "file" not in params:
        print >> sys.stderr, "Failed to create stream for RRD %d" % (index)
        print >> sys.stderr, "All RRDs must have a 'file' parameter"
        return
    
    if params['file'] in existing:
        return

    info = rrdtool.info(params['file'])
    minres = info['step']
    rows = info['rra[0].rows']
    print "Creating stream for RRD-%s %s: %s" % (rrdtype, params['file'], params['name'])


    if rrdtype == "smokeping":
        if "host" not in params:
            print >> sys.stderr, "Failed to create stream for RRD %d" % (index)
            print >> sys.stderr, "All Smokeping RRDs must have a 'host' parameter"
            return

        source = socket.gethostname()
        
        rrd_smokeping.insert_stream(db, None, params['name'], params['file'], 
                source, params['host'], minres, rows)
        
    if rrdtype == "muninbytes":
        if "switch" not in params:
            print >> sys.stderr, "Failed to create stream for RRD %d" % (index)
            print >> sys.stderr, "All MuninBytes RRDs must have a 'switch' parameter"
            return
        if "interface" not in params:
            print >> sys.stderr, "Failed to create stream for RRD %d" % (index)
            print >> sys.stderr, "All MuninBytes RRDs must have a 'interface' parameter"
            return
        if "direction" not in params:
            print >> sys.stderr, "Failed to create stream for RRD %d" % (index)
            print >> sys.stderr, "All MuninBytes RRDs must have a 'direction' parameter"
            return

        if params["direction"] not in ["sent", "received"]:
            print >> sys.stderr, "Failed to create stream for RRD %d" % (index)
            print >> sys.stderr, "'direction' parameter for MuninBytes RRDs must be either 'sent' or 'received'"
            return

        rrd_muninbytes.insert_stream(db, None, params['name'], params['file'], 
                params['switch'], params['interface'], params['direction'],
                minres, rows)

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
        print >> sys.stderr, "WARNING: %s does not exist - no RRD streams will be added" % (conf)
        return

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
                create_rrd_stream(db, subtype, parameters, index, files)
            parameters = {}
            subtype = x[1]
            index += 1
        else:
            parameters[x[0]] = x[1]

   
    if parameters != {}:
        create_rrd_stream(db, subtype, parameters, index, files)

    db.commit_transaction()
    f.close()


def run_module(rrds, config, exp):
    rrd = RRDModule(rrds, config, exp)
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
