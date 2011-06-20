'''
Auxiliar functions for logging.
'''

import datetime
import sys

def trace(output):
	print str(datetime.datetime.today()) + ' ' + output

def trace_err(output):
	sys.stderr.write(str(datetime.datetime.today()) + ' ' + output)
	sys.stderr.flush()
