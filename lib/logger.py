import sys
import logging

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
        print >> sys.stderr, msg
    else:
        logger.debug(msg) 


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :

