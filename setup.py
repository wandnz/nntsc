#!/usr/bin/env python

import sys

from pkg_resources import Requirement, resource_filename

try:
        from setuptools import setup
except ImportError:
        from distutils.core import setup

requires = [ \
        'python-rrdtool', 'psycopg2>=2.5', 'pika>=0.9.12', 'python-daemon', \
	'libnntsc-client', 'pywandevent' \
]

if sys.version_info < (2, 7):
        requires.append('argparse')

setup(name="nntsc",
	version="2.4",
	description='Nathan\'s Network Time Series Collector',
        author='Nathan Overall, Shane Alcock',
        author_email='contact@wand.net.nz',
        url='http://www.wand.net.nz',
	scripts=['build_nntsc_db', 'nntsc'],
	packages=['libnntsc', 'libnntsc.parsers'],
	install_requires = requires,
	package_dir = { \
		'libnntsc':'lib', \
		'libnntsc.parsers':'dataparsers', \
	},
	include_package_data=True,
	package_data = {
		'libnntsc': ['conf/nntsc.conf', 'conf/rrd.examples',
			'initscripts/nntsc', 'initscripts/nntsc.default']
	},
)

# XXX Commented out because this probably shouldn't be done here -- get the
# packaging system to do it instead


# Install configuration file
#filename = resource_filename(Requirement.parse("NNTSC"), "conf/nntsc.conf")

#try:
#	import shutil, os
#	if not os.path.exists("/etc/nntsc.conf"):
#		shutil.copyfile(filename, "/etc/nntsc.conf")
#except IOError:
#	print "Unable to copy configuration file to /etc/nntsc.conf"

