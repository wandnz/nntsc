from sqlalchemy import create_engine, Table, Column, Integer, \
    String, MetaData, ForeignKey, UniqueConstraint, Index
from sqlalchemy.types import Integer, String, Float, Boolean, BigInteger
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import text
import libnntsc.logger as logger

class PartitionedTable:
    def __init__(self, db, basetable, freq, indexcols):
        self.base = basetable
        self.db = db
        self.freq = freq
        self.currentstart = None
        self.currentend = None
        self.lastend = 0
        self.indexcols = indexcols
        self.triggername = self.base + "_trigger"
        self.existing = []

        for t in self.db.metadata.tables:
            if t[0:6] != "part__":
                continue
            comps = t.split("__")

            if (len(comps) != 4):
                continue

            if comps[1] != self.base:
                continue

            start = int(comps[2])
            end = int(comps[3])

            self.existing.append({'start':start, 'end':end, 'name':t})

            if end > self.lastend:
                self.lastend = end


    def _create_table(self, ts):
        
        start = ts / int(self.freq) * int(self.freq)
        self.currentstart = start
        self.currentend = start + self.freq
        name = "part__" + self.base + "__" + str(start) + "__" + str(self.currentend)

        logger.log("Creating new table partition: %s" % name)
            
        self.db.conn.execute("""CREATE TABLE %s ( CHECK (timestamp >= %d AND                    timestamp < %d ) ) INHERITS (%s);""" % 
                (name, self.currentstart, self.currentend, self.base))
   
        for col in self.indexcols:
            self.db.conn.execute("CREATE INDEX index_%s_%s ON %s (%s);" %
                (name, col, name, col))

        self.existing.append({'start':start, 'end':self.currentend, 'name':name})
        if self.currentend > self.lastend:
            self.lastend = self.currentend

        return name

    def update(self, ts):
        
        if self.currentstart != None and self.currentend != None:
            if ts >= self.currentstart and ts < self.currentend:
                return

        # Current table partition is not suitable, see if a suitable partition
        # exists already. If so, switch our trigger to use that. If not,
        # create a new table using our current desired frequency.
        #
        # Not creating a partition (i.e. using an existing one) should only
        # happen on NNTSC startup, such as when we read old data from a whole 
        # bunch of RRDs. Even in this case, the data should be sequential so
        # hopefully we aren't going to be switching partition frequently.
        # 
        # XXX Changing the frequency may result in partition overlap in certain
        # situations. Example: an old partition covered the time 100-120 but we 
        # changed the frequency to 100, restarted and got a measurement for 
        # 130. This would create a new partition for 100-200, which overlaps
        # with the earlier partition. If we later see data for 110, which 
        # table should it go into? I don't think we care, as long as data ends 
        # up in *a* partition that belongs to the parent table, but I am aware
        # that this will happen.

        name = None

        if ts < self.lastend:
            # Timestamp is before the end of our most recent partition, so
            # look for an existing partition that suits this data
            for p in self.existing:
                if ts >= p['start'] and ts < p['end']:
                    name = p['name']
                    self.currentstart = p['start']
                    self.currentend = p['end']
                    break

        if name == None:
            name = self._create_table(ts)

        #   self._create_table(name)
        logger.log("Switching to table partition: %s" % name)

        # Update our trigger that ensures we insert into the right partition
        trigfunc = text("""CREATE OR REPLACE FUNCTION %s()
                RETURNS TRIGGER AS $$
                BEGIN
                    INSERT INTO %s VALUES (NEW.*);
                    RETURN NULL;
                END;
                $$
                LANGUAGE plpgsql;
                """ % (self.base + "_trigfunc", name))
        self.db.conn.execute(trigfunc)

        # Create the trigger if it doesn't exist
        # XXX I really don't like this whole "drop and re-create" thing but
        # there is no 'CREATE OR REPLACE' or 'IF NOT EXISTS' for trigger
        # creation, which is ridiculous
        self.db.conn.execute("DROP TRIGGER IF EXISTS %s ON %s;" % 
                (self.triggername, self.base))
        trigger = text("""CREATE TRIGGER %s BEFORE INSERT ON %s
                FOR EACH ROW EXECUTE PROCEDURE %s();""" % 
                (self.triggername, self.base, self.base + "_trigfunc"))
        self.db.conn.execute(trigger)
        



# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :

