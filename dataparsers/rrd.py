from database import Database
from configurator import *
import sys, rrdtool, socket, time

def smokeping_stream_table():
    from sqlalchemy import Column
    from sqlalchemy.types import Integer, String

    return [
        # rrd filename
            Column('filename', String, nullable=False),
            Column('source', String, nullable=False),
            # host (fqdn or ip address)
            Column('host', String, nullable=False),
            # stream inside rrd
            #Column('substream', Integer, nullable = False),
            Column('minres', Integer, nullable=False, default=300),
            Column('highrows', Integer, nullable=False, default=1008),           
            # required so we don't miss any data from the rrd
            Column('lasttimestamp', Integer, nullable=False, default=0),
        ]

def smokeping_stream_constraints():
    return ['filename', 'source', 'host']

def smokeping_parse_db_row(row):
    pass
    
def smokeping_data_table():
    from sqlalchemy import Column
    from sqlalchemy.types import Integer, String, Float

    return [
        Column('uptime', Float, nullable=True),
        Column('loss', Integer, nullable=True),
        Column('median', Float, nullable=True),
        Column('ping1', Float, nullable=True),
        Column('ping2', Float, nullable=True),
        Column('ping3', Float, nullable=True),
        Column('ping4', Float, nullable=True),
        Column('ping5', Float, nullable=True),
        Column('ping6', Float, nullable=True),
        Column('ping7', Float, nullable=True),
        Column('ping8', Float, nullable=True),
        Column('ping9', Float, nullable=True),
        Column('ping10', Float, nullable=True),
        Column('ping11', Float, nullable=True),
        Column('ping12', Float, nullable=True),
        Column('ping13', Float, nullable=True),
        Column('ping14', Float, nullable=True),
        Column('ping15', Float, nullable=True),
        Column('ping16', Float, nullable=True),
        Column('ping17', Float, nullable=True),
        Column('ping18', Float, nullable=True),
        Column('ping19', Float, nullable=True),
        Column('ping20', Float, nullable=True)
    ]

def smokeping_insert_stream(db, name, fname, source, host, minres, rows):
    
    db.insert_stream(mod="rrd", modsubtype="smokeping", name=name, 
            filename=fname, source=source, host=host, minres=minres,
            highrows=rows, lasttimestamp=0)

def smokeping_insert_data(db, stream, ts, line):
    
    # This is terrible :(

    kwargs = {}
    line_map = {0:"uptime", 1:"loss", 2:"median", 3:"ping1", 4:"ping2",
        5:"ping3", 6:"ping4", 7:"ping5", 8:"ping6", 9:"ping7", 10:"ping8",
        11:"ping9", 12:"ping10", 13:"ping11", 14:"ping12", 15:"ping13", 
        16:"ping14", 17:"ping15", 18:"ping16", 19:"ping17", 20:"ping18",
        21:"ping19", 22:"ping20"}

    for i in range(0, len(line)):
        if line[i] == None:
            continue
        
        if i == 1:
            kwargs[line_map[i]] = int(float(line[i]))
        elif i > 1:
            kwargs[line_map[i]] = round(float(line[i]) * 1000.0, 6)
        else:
            kwargs[line_map[i]] = round(float(line[i]), 6)
    
    db.insert_data(mod="rrd", modsubtype="smokeping", stream_id=stream, \
            timestamp=ts, **kwargs)

"""
    db.insert_data(mod="rrd", modsubtype="smokeping", stream_id=stream, \
            timestamp=ts, uptime=float(line[0]), loss=int(float(line[1])),
            median=float(line[2]), ping1=float(line[3]), ping2=float(line[4]),
            ping3=float(line[5]), ping4=float(line[6]), ping5=float(line[7]),
            ping6=float(line[8]), ping7=float(line[9]), ping8=float(line[10]),
            ping9=float(line[11]), ping10=float(line[12]), 
            ping11=float(line[13]),ping12=float(line[14]), 
            ping13=float(line[15]), ping14=float(line[16]),
            ping15=float(line[17]), ping16=float(line[18]), 
            ping17=float(line[19]), ping18=float(line[20]), 
            ping19=float(line[21]), ping20=float(line[22]))
  """  

class RRDModule:
    def __init__(self, rrds, config):

        nntsc_conf = load_nntsc_config(config)
        if nntsc_conf == 0:
            return 0

        dbconf = get_nntsc_db_config(nntsc_conf)
        if dbconf == {}:
            sys.exit(1)

        self.db = Database(dbconf["name"], dbconf["user"], dbconf["pass"], 
                dbconf["host"])

        
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

        if (r["lasttimestamp"] == 0):
            startts = endts - (r['highrows'] * r['minres'])

        else:
            startts = r["lasttimestamp"]

        return startts, endts

    def run(self):
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
                            smokeping_insert_data(self.db, r['stream_id'],
                                    current, line)
                        
                        if current > r['lasttimestamp']:
                            r['lasttimestamp'] = current
                        current += step
                    
                    # TODO: Update the lasttimestamp in the db
                    self.db.update_timestamp(r['stream_id'], r['lasttimestamp']) 
            # TODO: COMMIT!
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
            smokeping_insert_stream(db, name, rrd, source, host, minres, rows)
        
        name = None
        rrd = None
        host = None
        subtype = None

    f.close()

def run_module(rrds, config):
    rrd = RRDModule(rrds, config)
    rrd.run()
    

def tables():
    res = {}
    res["rrd_smokeping"] = (smokeping_stream_table(), smokeping_stream_constraints(), smokeping_data_table())

    return res

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
