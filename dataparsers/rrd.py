from sqlalchemy import create_engine, Table, Column, Integer, \
        String, MetaData, ForeignKey, UniqueConstraint
from sqlalchemy.types import Integer, String, Float
from libnntsc.database import Database
from libnntsc.configurator import *
from libnntsc.parsers import rrd_smokeping
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
        self.rrds = {}
        for r in rrds:
            if r['modsubtype'] == 'smokeping':
                self.smokepings[r['stream_id']] = r
            
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
                        
                        if current > r['lasttimestamp']:
                            r['lasttimestamp'] = current
                        current += step
                    
                    self.db.update_timestamp(r['stream_id'], r['lasttimestamp']) 
            self.db.commit_transaction()

            time.sleep(30)

def insert_rrd_streams(db, conf):

    if conf == "":
        return

    try:
        f = open(conf, "r")
    except IOError, e:
        print >> sys.stderr, "WARNING: %s does not exist - no smokeping streams will be added" % (conf)
        return

    name = None
    rrd = None
    host = None
    subtype = None

    for line in f:
        if line[0] == '#':
            continue
        if line == "\n" or line == "":
            continue

        x = line.strip().split("=")
        if len(x) != 2:
            continue
        if x[0] == "name":
            name = x[1]
        if x[0] == "host":
            host = x[1]
        if x[0] == "file" or x[0] == "rrd":
            rrd = x[1]
        if x[0] == "type":
            subtype = x[1]

        # Keep going until we get a full set of options
        if name == None or rrd == None or host == None:
            continue

        info = rrdtool.info(rrd)
        minres = info['step']
        rows = info['rra[0].rows']

        source = socket.gethostname()

        print "Creating stream for RRD-%s %s: %s" % (subtype, rrd, name)
        
        if subtype == "smokeping":
            rrd_smokeping.insert_stream(db, None, name, rrd, source, host, 
                    minres, rows)
        
        name = None
        rrd = None
        host = None
        subtype = None

    f.close()

def run_module(rrds, config, exp):
    rrd = RRDModule(rrds, config, exp)
    rrd.run()
    

def tables(db):
    
    st_name = rrd_smokeping.stream_table(db)
    dt_name = rrd_smokeping.data_table(db)

    db.register_collection("rrd", "smokeping", st_name, dt_name)
        
    #res = {}
    #res["rrd_smokeping"] = (smokeping_stream_table(), smokeping_stream_constraints(), smokeping_data_table())

    #return res

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
