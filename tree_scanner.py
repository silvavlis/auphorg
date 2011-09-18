'''scans the specified directory looking for files to be added to the list of photos'''
import os.path
import files_handler
import logging
import multiprocessing

lock = None
processed = None

def init_process(inherited_lock, inherited_processed):
	lock = inherited_lock
	processed = inherited_processed

def file_processor(filepath):
	logger = logging.getLogger('AuPhOrg')
	logger.info('starting process for file %s' % filepath)
	fsh = files_handler.FilesHandler()
	fsh.add_file(unicode(filepath, 'utf-8'))
	lock.acquire()
	processed.value += 1
	lock.release()
	logger.info('leaving process for file %s' % filepath)

# scans the given tree and tries to add the found files to the DB
class TreeScanner():
	_pool = None
	def __init__(self):
		self._logger = logging.getLogger('AuPhOrg')
		self._files_to_add = []

	def __del__(self):
		self._logger.info('done processing tree')

	def init_pool(self):
		self.n_cpus = multiprocessing.cpu_count()
		if TreeScanner._pool != None:
			raise RuntimeError
		self._logger.info('starting the pool of processes')
		TreeScanner._pool = multiprocessing.Pool(self.n_cpus)
		self._logger.info('pool of processes started')

	def scan_tree(self, photostree_root):
		self._logger.info('processing tree %s' % photostree_root)
		os.path.walk(photostree_root, self._scan_subtree, None)
		n_files_to_add = len(self._files_to_add)
		lock = multiprocessing.Lock()
		result = TreeScanner._pool.map_async(file_processor, self._files_to_add)
		result.wait(10)
		while not result.ready():
			percent = 100 * processed.value / n_files_to_add
			print "%d out of %d ready (%d%%)" % \
				(processed.value, n_files_to_add, percent)
			result.wait(10)
		self._logger.info('the pool of processes already procesed the tree!')

	def _scan_subtree(self, arg, dir_path, filenames):
		for filename in filenames:
			filepath = os.path.join(dir_path, filename)
			if not (os.path.isdir(filepath) or os.path.islink(filepath)):
				self._files_to_add.append(filepath)

if __name__ == '__main__':
	lock = multiprocessing.Lock()
	processed = multiprocessing.Value('i', 0)
	log_fh = logging.FileHandler('./auphorg.log')
	log_so = logging.StreamHandler()
	formatter = logging.Formatter('%(asctime)s %(levelname)s %(module)s: %(message)s')
	log_fh.setFormatter(formatter)
	log_so.setFormatter(formatter)
	logger = logging.getLogger('AuPhOrg')
	logger.addHandler(log_fh)
#	logger.addHandler(log_so)
	logger.setLevel(logging.INFO)
	tree_scanner = TreeScanner()
	tree_scanner.init_pool()
	tree_scanner.scan_tree('/home/silvavlis/Pictures')
