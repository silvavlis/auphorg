#!/usr/bin/python
# -*- coding: utf-8 -*-

'''scans the specified directory looking for files to be added to the list of photos'''
import sys
import os.path
import files_handler
import logging
import multiprocessing
import optparse

lock = None
processed = None

# processes a single file
def file_processor(filepath):
	'process the given file'
	logger = logging.getLogger('AuPhOrg')
	logger.info('starting process for file %s' % filepath)
	lock.acquire()
	fsh = files_handler.FilesHandler(TreeScanner.db_path)
	lock.release()
	fsh.add_file(unicode(filepath, 'utf-8'))
	lock.acquire()
	processed.value += 1
	lock.release()
	logger.info('leaving process for file %s' % filepath)

# scans the given tree and tries to add the found files to the DB
class TreeScanner():
	_pool = None
	db_path = ""
	def __init__(self):
		self._logger = logging.getLogger('AuPhOrg')
		self._files_to_add = []

	def __del__(self):
		self._logger.info('done processing tree')

	def init_pool(self, database_path = ""):
		TreeScanner.db_path = database_path
		self.n_cpus = multiprocessing.cpu_count()
		if TreeScanner._pool != None:
			raise RuntimeError
		self._logger.info('starting a pool of %d processes' % self.n_cpus)
		TreeScanner._pool = multiprocessing.Pool(self.n_cpus)
		self._logger.info('pool of processes started')

	def scan_tree(self, photostree_root):
		self._logger.info('analyzing tree %s' % photostree_root)
		os.path.walk(photostree_root, self._scan_subtree, None)
		n_files_to_add = len(self._files_to_add)
		self._logger.info('tree analyzed (%s files to process)' % n_files_to_add)
		self._logger.info('processing tree')
		#lock = multiprocessing.Lock()
		result = TreeScanner._pool.map_async(file_processor, self._files_to_add)
		result.wait(10)
		while not result.ready():
			percent = 100 * processed.value / n_files_to_add
			print "%d out of %d ready (%d%%)" % \
				(processed.value, n_files_to_add, percent)
			result.wait(120)
		self._logger.info('the pool of processes already procesed the tree!')

	def _scan_subtree(self, arg, dir_path, filenames):
		for filename in filenames:
			filepath = os.path.join(dir_path, filename)
			if not (os.path.isdir(filepath) or os.path.islink(filepath)):
				self._files_to_add.append(filepath)

if __name__ == '__main__':
	# initialize the logging infrastructure
	log_fh = logging.FileHandler('./auphorg.log')
	log_so = logging.StreamHandler()
	formatter = logging.Formatter('%(asctime)s %(levelname)s [%(processName)s]%(module)s: %(message)s')
	log_fh.setFormatter(formatter)
	log_so.setFormatter(formatter)
	logger = logging.getLogger('AuPhOrg')
	logger.addHandler(log_fh)
#	logger.addHandler(log_so)
	# get the arguments
	parser = optparse.OptionParser()
	parser.add_option('-r', '--root', dest='tree_root', metavar='ROOT', \
		help='scan the tree recursively starting from ROOT')
	parser.add_option('-v', '--verbosity', dest='verbosity', metavar='VERBOSITY_LEVEL', \
		help='set the verbosity level to VERBOSITY_LEVEL')
	parser.add_option('-d', '--db', dest='db_path', metavar='DATABASE_PATH', \
		help='use the database that can be found in DATABASE_PATH or create it new there')
	(options, args) = parser.parse_args()
	if options.tree_root == None:
		print "The root directory of the tree to be scanned is required!"
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
	logger.setLevel(verbosity)
	if options.db_path == None:
		logger.warning('no DB specified in the command line')
		options.db_path = ""
	# initialize the variables required for keeping track of the number of processed files
	lock = multiprocessing.Lock()
	processed = multiprocessing.Value('i', 0)
	# start processing the tree
	tree_scanner = TreeScanner()
	tree_scanner.init_pool(options.db_path)
	tree_scanner.scan_tree(options.tree_root)
