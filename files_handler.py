import Image
import wave
import hashlib
import os.path 
import db_backend
import subprocess
import logging

TAGS_TO_GET = [
				'Model',
				'Software',
				'DateTimeOriginal',
				'CreateDate',
				'ImageWidth',
				'ImageHeight',
				'TagsList',
				'HierarchicalSubject',
				'Subject',
				'Keywords'
				]
EXIF_TOOL = "/usr/bin/exiftool"
EXIFTOOL_REQUEST = EXIF_TOOL + ' -s'
for tag in TAGS_TO_GET:
	EXIFTOOL_REQUEST = EXIFTOOL_REQUEST + ' -' + tag
CHECKSUM_TOOL = "/usr/bin/sha1sum"

logger_file = logging.getLogger('AuPhOrg')
logger_output = logging.getLogger('StdOutput')

class ApoFileError(TypeError):
	'superclass for errors in the files_handler module'
	def __init__(self):
		pass

class ApoFileUnknown(ApoFileError):
	'error unknown file format'
	def __init__(self, filename):
		super(ApoFileUnknown, self).__init__()
		self.filename = filename
		self.fileext = os.path.splitext(filename)[1][1:]

	def __str__(self):
		err_msg = 'file %s cannot be handled, because its extension (%s) is not supported' % \
			(self.filename, self.fileext)
		logger_file.error = err_msg
		logger_output.error = err_msg
		return err_msg

class FilesHandler:
	'handles the specified multimedia files'
	ignore_exts = ('.db', '.strm')

	def __init__(self, lock, database_path = ""):
		'initializes the object'
		self._lock = lock
		if database_path == '':
			logger_file.debug('no DB specified in the command line')
		else:
			logger_file.debug('instanciating the DB backend for %s' % database_path)
		self._db = db_backend.DbConnector(lock, database_path)
		logger_file.debug('DB backend instanciated')

	def __del__(self):
		'close the database object'
		self._db = None

	def _file_checksum(self, path):
		'calculates the SHA1 checksum of the file'
		logger_file.debug('calculating the checksum of the file %s' % path)
		cmd = CHECKSUM_TOOL + ' -b "' + path + '"'
		output = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout
		lines = output.read().splitlines()
		logger_file.debug('checksum of file calculated')
		return lines[0].split(' ')[0]

	def _file_info(self, path):
		'gets the information of the file that will be saved in the DB'
		# calculate the checksum
		checksum = self._file_checksum(path)
		# get the timestamp of the last modification
		time = os.path.getmtime(path)
		# get the size
		size = os.path.getsize(path)
		# return the values
		return (checksum, time, size)

	def _image_checksum(self, path):
		'calculates the SHA512 checksum of the contained image'
		logger_file.debug('calculating the checksum of the image contained in file %s' % path)
		try:
			img = Image.open(path)
			cksm = hashlib.sha512()
			cksm.update(img.tostring())
			logger_file.debug('checksum of image calculated')
			return cksm.hexdigest()
		except Exception, err:
			logger_file.error('Error gettig image from file %s: %s' % (path, str(err)))
			logger_output.error('Error gettig image from file %s: %s' % (path, str(err)))

	def _add_jpeg(self, item_name, path):
		'adds a JPEG file to the DB'
		logger_file.debug('adding the JPEG file %s to the item %s' % (path, item_name))
		# get the tags of the file
		exif_tags = {}
		cmd = EXIFTOOL_REQUEST + ' "' + path + '"'
		output = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout
		lines = output.read().splitlines()
		for line in lines:
			(tag_name, _, tag_value) = line.partition(':')
			exif_tags[unicode(tag_name.strip(), 'iso-8859-15')] = unicode(tag_value.strip(), 'iso-8859-15')
		# calculate the checksum of the image
		content_checksum = self._image_checksum(path)
		# get the required file information
		(file_checksum, file_time, file_size) = self._file_info(path)
		# add the file to the DB and to the item
		self._db.add_rich_file(path, file_time, file_size, file_checksum, content_checksum, exif_tags)
		self._db.add_item_tags(item_name, path)
		logger_file.debug('JPEG file added to item')

	def _add_poor_file(self, item_type, item_name, path, cntt_cksm_fnt):
		'adds a poor file to the DB'
		logger_file.debug('adding the %s file %s to the item %s' % (item_type, path, item_name))
		# get the required file information
		(file_checksum, file_time, file_size) = self._file_info(path)
		# calculate the checksum of the file content, if possible
		if cntt_cksm_fnt == None:
			content_checksum = file_checksum
		else:
			content_checksum = cntt_cksm_fnt(path)
		# add the file to the DB
		self._db.add_poor_file(path, file_time, file_size, file_checksum, content_checksum)
		self._db.add_item_content(item_name, path)
		logger_file.debug('%s file added to item' % item_type)

	def _video_checksum(self, path):
		'calculates the checksum of a video, using ffmpeg and md5sum'
		logger_file.debug('calculating the checksum of the video contained in file %s' % path)
		cmd = '/usr/bin/ffmpeg -i "' + path + '" -f avi - 2> /dev/null | /usr/bin/sha512sum -b'
		output = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout
		result = output.read().splitlines()[0]
		logger_file.debug('checksum of video calculated')
		return result.split(' ')[0]

	def _wav_checksum(self, path):
		'calculates the checksum of a wave file'
		logger_file.debug('calculating the checksum of the audio contained in file %s' % path)
		try:
			wav = wave.open(path, 'rb')
			cksm = hashlib.sha512()
			cksm.update(wav.readframes(wav.getnframes()))
			logger_file.debug('checksum of audio calculated')
			return cksm.hexdigest()
		except Exception, err:
			logger_file.error('Error getting audio from wave file %s: %s' % (path, str(err)))
			logger_output.error('Error getting audio from wave file %s: %s' % (path, str(err)))

	def is_older(self, path):
		pass

	def add_file(self, path):
		'adds the file of the given path to the DB'
		logger_file.debug('adding the file %s' % path)
		# test that path is file
		if not os.path.isfile(path):
			raise RuntimeError, "path isn't a file!"
		# get file extension to find out type of file
		(item_name, extension) = os.path.splitext(path)
		extension = extension.lower()
		# create a new item for the file (it does nothing if it already exists)
		self._db.add_item(item_name, force=False)
		# add the file to the corresponding item
		if (extension in ('.jpg', '.jpeg', '.thm', '.jpe', '.jpg_original')):
			self._add_jpeg(item_name, path)
		elif (extension in ('.avi', '.mov', '.wmv')):
			self._add_poor_file('video', item_name, path, self._video_checksum)
		elif (extension in ('.raw', '.rw2')):
			self._add_poor_file('RAW', item_name, path, None)
		elif (extension in ('.tif')):
			self._add_poor_file('TIFF', item_name, path, self._image_checksum)
		elif (extension in ('.wav')):
			self._add_poor_file('audio', item_name, path, self._wav_checksum)
		elif (extension in self.ignore_exts):
			logger_file.debug('Ignore file')
		else:
			raise ApoFileUnknown(path)
		logger_file.debug('file added')
