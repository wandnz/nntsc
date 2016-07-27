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
import logging
import time

backgrounded = False
logger = None

def createLogger(flag, logfile, name):
    global backgrounded, logger
    backgrounded = flag

    if backgrounded:
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler = logging.FileHandler(logfile)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        #logging.basicConfig(filename="/tmp/nntsc.log", format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
    
def log(msg):

    if not backgrounded:
        print >> sys.stderr, time.strftime("%d %b %Y %H:%M:%S"), msg
    else:
        logger.debug(msg) 


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :

