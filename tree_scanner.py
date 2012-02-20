#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import os.path
import files_handler
import logging
import multiprocessing
import optparse

lock = None
processed = None

logger_file = logging.getLogger('AuPhOrg')
logger_output = logging.getLogger('StdOutput')

# processes a single file
def file_processor(filepath):
	'process the given file'
	filepath = unicode(filepath, 'utf-8')
	logger_file.debug('adding file %s' % filepath)
	logger_output.debug('adding file %s' % filepath)
#	# check if the file has been already added previously to the DB
#	db = db_backend.DbConnector(TreeScanner.db_path, lock)
#	logger_file.info('hi %s' % filepath)
#	#if db.file_exists(unicode(filepath, 'utf-8')):
#	if db.file_exists(filepath):
#		logger_file.info('file %s was already in the DB' % filepath)
#		return
#	else:
#		logger_file.info('file %s was not in the DB yet' % filepath)
#	# here if file not in DB, add it
	logger_file.debug('acquiring lock')
	lock.acquire()
	logger_file.debug('lock acquired')
	fsh = files_handler.FilesHandler(lock, TreeScanner.db_path)
	lock.release()
	logger_file.debug('lock released')
	fsh.add_file(filepath)
	logger_file.debug('acquiring lock')
	lock.acquire()
	logger_file.debug('lock acquired')
	processed.value += 1
	lock.release()
	logger_file.debug('lock released')
	logger_file.info('done adding file %s' % filepath)

# scans the given tree and tries to add the found files to the DB
class TreeScanner():
	'scans a tree'
	_pool = None
	db_path = ""

	def __init__(self):
		'initializes the tree scanner'
		self._files_to_add = []

	def __del__(self):
		'informs about the end of the processing'
		logger_file.info('done processing tree')

	def init_pool(self, database_path = ""):
		'initializes the pool of processes that will process the individual files'
		TreeScanner.db_path = database_path
		self.n_cpus = multiprocessing.cpu_count()
		if TreeScanner._pool != None:
			raise RuntimeError
		logger_file.info('starting a pool of %d processes' % self.n_cpus)
		TreeScanner._pool = multiprocessing.Pool(self.n_cpus)
		logger_file.debug('pool of processes started')

	def scan_tree(self, photostree_root):
		'walks the tree assigning the files to be processes to the pool of processes'
		logger_file.info('analyzing tree %s' % photostree_root)
		logger_output.info('analyzing tree %s' % photostree_root)
		os.path.walk(photostree_root, self._process_dir, None)
		n_files_to_add = len(self._files_to_add)
		logger_file.info('tree analyzed: %s files to process' % n_files_to_add)
		logger_output.info('tree analyzed: %s files to process' % n_files_to_add)
		logger_file.debug('processing tree')
		result = TreeScanner._pool.map_async(file_processor, self._files_to_add)
		result.wait(10)
		while not result.ready():
			percent = 100 * processed.value / n_files_to_add
			logger_output.info('%d out of %d ready (%d%%)' % \
				(processed.value, n_files_to_add, percent))
			result.wait(120)
		logger_file.debug('the pool of processes already processed the tree!')
		logger_output.info('done processing the tree!')

	def _process_dir(self, arg, dir_path, filenames):
		'processes the files found in the given directory'
		for filename in filenames:
			filepath = os.path.join(dir_path, filename)
			if not (os.path.isdir(filepath) or os.path.islink(filepath)):
				self._files_to_add.append(filepath)

# configure a logger
def config_logger(log_handler, log_format, logger_name, logging_level = logging.INFO):
	'configures a logger'
	formatter = logging.Formatter(log_format)
	log_handler.setFormatter(formatter)
	logger = logging.getLogger(logger_name)
	logger.addHandler(log_handler)
	logger.setLevel(logging_level)
	return logger

# parse the command line arguments
def parse_args():
	'parses the arguments to get the verbosity, the tree root and the DB path'
	parser = optparse.OptionParser()
	parser.add_option('-r', '--root', dest='tree_root', metavar='ROOT', \
		help='scan the tree recursively starting from ROOT')
	parser.add_option('-v', '--verbosity', dest='verbosity', metavar='VERBOSITY_LEVEL', \
		help='set the verbosity level to VERBOSITY_LEVEL')
	parser.add_option('-d', '--db', dest='db_path', metavar='DATABASE_PATH', \
		help='use the database that can be found in DATABASE_PATH or create it new there')
	(options, args) = parser.parse_args()
	if options.tree_root == None:
		logger_file.error("The root directory of the tree to be scanned is required! (option '-r')")
		logger_output.error("The root directory of the tree to be scanned is required! (option '-r')")
		sys.exit()
	if options.verbosity == '0':
		verbosity = logging.CRITICAL
	elif options.verbosity == '1':
		verbosity = logging.ERROR
	elif options.verbosity == '2':
		verbosity = logging.WARNING
	elif options.verbosity == '3':
		verbosity = logging.INFO
	elif options.verbosity == '4':
		verbosity = logging.DEBUG
	else:
		verbosity = logging.ERROR
	logger_file.setLevel(verbosity)
	if options.db_path == None:
		logger_file.error("no DB specified in the command line (option '-d')")
		logger_output.error("no DB specified in the command line (option '-d')")
		sys.exit()
	logger_output.info("tree to scan => %s" % options.tree_root)
	logger_output.info("path of the DB => %s" % options.db_path)
	return (options.tree_root, options.db_path)

#command line execution
if __name__ == '__main__':
	# output a separator to both the logging file and the standard output
	sep_format = '**************** %(asctime)s %(message)s ****************'
	log_filename = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'auphorg.log')
	log_file = logging.FileHandler(log_filename)
	logger_file = config_logger(log_file, sep_format, 'AuPhOrg', logging.DEBUG)
	logger_file.info('New log entry')
	log_output = logging.StreamHandler(sys.stdout)
	logger_output = config_logger(log_output, sep_format, 'StdOutput')
	logger_output.info('Starting execution')
	# configure logging for normal logging
	log_format = '%(asctime)s %(levelname)s [%(processName)s]>%(module)s: %(message)s'
	logger_file = config_logger(log_file, log_format, 'AuPhOrg')
	log_format = '%(asctime)s > %(message)s'
	logger_output = config_logger(log_output, log_format, 'StdOutput')
	# parse the arguments
	(tree_root, db_path) = parse_args()
	# initialize the variables required for keeping track of the number of processed files
	lock = multiprocessing.Lock()
	#TODO: WHAT HAPPENS WITH THE LOCKS!!
	if lock.acquire(False) == False:
		logger_file.warning("lock wasn't properly released by a previous run, releasing it now")
	lock.release()
	processed = multiprocessing.Value('i', 0)
	# start processing the tree
	tree_scanner = TreeScanner()
	tree_scanner.init_pool(db_path)
	tree_scanner.scan_tree(tree_root)
