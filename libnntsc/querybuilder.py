#
# This file is part of NNTSC.
#
# Copyright (C) 2013-2017 The University of Waikato, Hamilton, New Zealand.
#
# Authors: Shane Alcock
#          Brendon Jones
#
# All rights reserved.
#
# This code has been developed by the WAND Network Research Group at the
# University of Waikato. For further information please see
# http://www.wand.net.nz/
#
# NNTSC is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 as
# published by the Free Software Foundation.
#
# NNTSC is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with NNTSC; if not, write to the Free Software Foundation, Inc.
# 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#
# Please report any bugs, questions or comments to contact@wand.net.nz
#

from libnntscclient.logger import *

class QueryBuilder:
    def __init__(self):
        self.clauses = {}

    def add_clause(self, name, clause, params=None):
        if params is None:
            params = []
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
