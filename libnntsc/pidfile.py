# This file is part of NNTSC, however the code in it was not developed by the
# authors of NNTSC.
#
# Copyright (C) 2011 Graham Poulter
#
# This code is subject to the terms of the MIT license, which is described in
# full in the file COPYING that should be included with the NNTSC source
# distribution.
#
# If you did not receive a copy of the MIT license with this code, please
# email us at contact@wand.net.nz
#
# $Id$

"""This code was written by Graham Poulter and distributed using the MIT
   license.

   It was originally acquired from:
   http://code.activestate.com/recipes/577911-context-manager-for-a-daemon-pid-file/
"""

import fcntl
import os

class PidFile(object):
    """Context manager that locks a pid file.  Implemented as class
    not generator because daemon.py is calling .__exit__() with no parameters
    instead of the None, None, None specified by PEP-343."""
    # pylint: disable=R0903

    def __init__(self, path):
        self.path = path
        self.pidfile = None

    def __enter__(self):
        try:
		self.pidfile = open(self.path, "a+")
	except IOError:
		raise SystemExit("Failed to open pid file: %s" % self.path)
        try:
            fcntl.flock(self.pidfile.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            raise SystemExit("Already running according to " + self.path)
        self.pidfile.seek(0)
        self.pidfile.truncate()
        self.pidfile.write(str(os.getpid()))
        self.pidfile.flush()
        self.pidfile.seek(0)
        return self.pidfile

    def __exit__(self, exc_type=None, exc_value=None, exc_tb=None):
        try:
            self.pidfile.close()
        except IOError as err:
            # ok if file was just closed elsewhere
            if err.errno != 9:
                raise
        os.remove(self.path)
