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


import sys

nntsc_hdr_fmt = "!BBL"
nntsc_req_fmt = "!LLQ"

NNTSC_CLIENTAPI_VERSION="1.5.0"

NNTSC_REQ_COLLECTION = 0
NNTSC_REQ_STREAMS = 1
NNTSC_REQ_SCHEMA = 2
NNTSC_REQ_ACTIVE_STREAMS = 3

NNTSC_REQUEST = 0
NNTSC_COLLECTIONS = 1
NNTSC_SCHEMAS = 2
NNTSC_STREAMS = 3
NNTSC_SUBSCRIBE = 4
NNTSC_HISTORY = 5
NNTSC_LIVE = 6
NNTSC_AGGREGATE = 7
NNTSC_PERCENTILE = 8
NNTSC_PUSH = 9
NNTSC_ACTIVE_STREAMS = 10
NNTSC_REGISTER_COLLECTION = 11
NNTSC_VERSION_CHECK=12
NNTSC_HISTORY_DONE=13
NNTSC_QUERY_CANCELLED=14
# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :
