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

DB_NO_ERROR = 0
DB_DATA_ERROR = -1
DB_GENERIC_ERROR = -2
DB_INTERRUPTED = -3
DB_OPERATIONAL_ERROR = -4
DB_CODING_ERROR = -5
DB_DUPLICATE_KEY = -6
DB_NO_CURSOR = -7
DB_QUERY_TIMEOUT = -8

class DBQueryException(Exception):
    def __init__(self, code):
        self.code = code
    def __str__(self):
        if self.code == DB_DATA_ERROR:
            return "Data error detected during NNTSC query"
        if self.code == DB_GENERIC_ERROR:
            return "Generic database error detected during NNTSC query"
        if self.code == DB_INTERRUPTED:
            return "Keyboard interrupt detected during NNTSC query"
        if self.code == DB_CODING_ERROR:
            return "Bad database code encountered during NNTSC query"
        if self.code == DB_DUPLICATE_KEY:
            return "Duplicate key error while performing NNTSC query"
        if self.code == DB_QUERY_TIMEOUT:
            return "NNTSC database query timed out"
        if self.code == DB_OPERATIONAL_ERROR:
            return "Connection to NNTSC database was lost"
        if self.code == DB_NO_CURSOR:
            return "Could not execute query as had no valid cursor"
        if self.code == DB_NO_ERROR:
            return "No error occurred, why are we getting this exception?"
        return "Unknown error code for DBQueryException: %d" % (code)

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
