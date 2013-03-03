#!/usr/bin/env python

import sys

try:
        from setuptools import setup
except ImportError:
        from distutils.core import setup

requires = [ \
        'sqlalchemy', 'py_rrdtool', 'psycopg2' \
]

if sys.version_info < (2, 7):
        requires.append('argparse')

setup(name="NNTSC",
	version="2.0",
	description='Nathan\'s Network Time Series Collector',
        author='Nathan Overall, Shane Alcock',
        author_email='contact@wand.net.nz',
        url='http://www.wand.net.nz',
	scripts=['build_db', 'datacollector'],
	packages=['libnntsc', 'libnntsc.parsers', 'pywandevent'],
	install_requires = requires,
	package_dir = { \
		'libnntsc':'lib', \
		'libnntsc.parsers':'dataparsers', \
		'pywandevent':'pywandevent',
	},
	include_package_data=True,
	package_data = {
		'libnntsc': ['conf/example.conf']
	}
)
