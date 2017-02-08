import psycopg2
import psycopg2.extras
from libnntscclient.logger import *
from libnntsc.parsers import amp_traceroute
import time

class QueryBuilder:
    def __init__(self):
        self.clauses = {}

    def add_clause(self, name, clause, params=[]):
        self.clauses[name] = (clause, params)

    def reset(self):
        self.clauses = {}

    def create_query(self, order):
        paramlist = []
        querystring = ""

        for cl in order:
            paramlist += self.clauses[cl][1]
            querystring += self.clauses[cl][0] + " "

        return querystring, tuple(paramlist)


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
