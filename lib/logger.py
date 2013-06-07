import sys
import logging

backgrounded = False
logger = None

def setBackgrounded(flag):
    backgrounded = flag

    if backgrounded:
        #logger = logging.getLogger("NNTSCLogger")
        #logger.setLevel(logging.DEBUG)
        #formatter = logging.Formatter("(asctime)s - %(name)s - %(levelname)s - %(message)s")
        #handler = logging.FileHandler("/tmp/nntsc.log")
        #handler.setFormatter(formatter)
        #logger.addHandler(handler)

        logging.basicConfig(file="/tmp/nntsc.log", format="(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.DEBUG)
    
def log(msg):

    if not backgrounded:
        print >> sys.stderr, msg
    else:
        logging.debug(msg) 


# vim: set sw=4 tabstop=4 softtabstop=4 expandtab :

