#!/usr/bin/env python

import sys

try:
        from setuptools import setup
except ImportError:
        from distutils.core import setup

requires = [ \
        'sqlalchemy>=0.8', 'py_rrdtool', 'psycopg2', 'pika' \
]

if sys.version_info < (2, 7):
        requires.append('argparse')

setup(name="NNTSC",
	version="2.1",
	description='Nathan\'s Network Time Series Collector',
        author='Nathan Overall, Shane Alcock',
        author_email='contact@wand.net.nz',
        url='http://www.wand.net.nz',
	scripts=['build_db', 'nntsc'],
	packages=['libnntsc', 'libnntsc.parsers', 'pywandevent', \
		'libnntsc.client'],
	install_requires = requires,
	package_dir = { \
		'libnntsc':'lib', \
		'libnntsc.parsers':'dataparsers', \
		'pywandevent':'pywandevent',
		'libnntsc.client':'clientapi'
	},
	include_package_data=True,
	package_data = {
		'libnntsc': ['conf/example.conf', 'conf/rrd.examples']
	}
)

