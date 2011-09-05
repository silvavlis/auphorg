'''scans the specified directory looking for files to be added to the list of photos'''
import os.path
import files_handler
import logging

# scans the given tree and tries to add the found files to the DB
class TreeScanner(list):
	def __init__(self, photostree_root):
		self._logger = logging.getLogger('AuPhOrg')
		self._logger.info('processing tree %s' % photostree_root)
		self._fsh = files_handler.FilesHandler()
		os.path.walk(photostree_root, self._scan_subtree, None)
		self._logger.info('done processing tree')

	def __del__(self):
		self._fsh = None

	def _scan_subtree(self, arg, dir_path, filenames):
		for filename in filenames:
			filepath = os.path.join(dir_path, filename)
			if not (os.path.isdir(filepath) or os.path.islink(filepath)):
				self._fsh.add_file(unicode(filepath, 'utf-8'))
