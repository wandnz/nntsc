import os, sys

import libnntsc.parsers

def import_parsers():
	from libnntsc.parsers import *
	modules = {}

	for i in libnntsc.parsers.__all__:
		name = i
		if name == "lpi" or name == "tunnelusers":
			continue
		modules[name] = sys.modules['libnntsc.parsers.' + name]
	return modules

