import psycopg2
import psycopg2.extras
import sys

from libnntsc.dbselect import DBSelector
from libnntsc.database import DBInsert

if len(sys.argv) < 2:
    print >> sys.stderr, "Usage: %s <database name>" % (sys.argv[0])
    sys.exit(1)

dbname = sys.argv[1]

db = DBSelector("tracerouteupgrade", dbname)
db.connect_db(10)

collections = db.list_collections()

colid = None
for col in collections:
    if col['module'] == "amp" and col['modsubtype'] == "traceroute":
        colid = col['id']
        break

if colid is None:
    print >> sys.stderr, "Error: no amp-traceroute collection in database"
    sys.exit(1)

streams = db.select_streams_by_collection(colid, 0)

db = DBInsert(dbname)
db.connect_db(10)

# Create base aspath table
aspathcols = [ \
    {"name":"aspath_id", "type":"serial primary key"},
    {"name":"aspath", "type":"varchar[]", "null":False, "unique":True}
]

db.create_misc_table("data_amp_traceroute_aspaths", aspathcols)

query = "ALTER TABLE data_amp_traceroute ADD COLUMN aspath_id integer"
db._streamsquery(query)

for s in streams:
    datatable = "data_amp_traceroute_%d" % (s['stream_id'])
    aspathtable = "data_amp_traceroute_aspaths_%d" % (s['stream_id'])    

    db.clone_table("data_amp_traceroute_aspaths", s['stream_id'])

    query = "ALTER TABLE %s ADD COLUMN aspath_id integer" % (datatable)
    db._streamsquery(query)

    db.add_foreign_key(datatable, "aspath_id", aspathtable, "aspath_id")
    db.commit_streams()
    

# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :

