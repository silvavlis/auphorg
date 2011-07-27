import Image
import wave
import hashlib
import os.path 
import db_backend
import subprocess
import logging

TAGS_CLASSIFICATION = [
				'FileName',
				'Model',
				'Software',
				'DateTimeOriginal',
				'CreateDate',
				'ImageWidth',
				'ImageHeight'
				]
TAGS_TO_GET = TAGS_CLASSIFICATION + [
				'TagsList',
				'HierarchicalSubject',
				'Subject',
				'Keywords'
				]
EXIF_TOOL = "/usr/bin/exiftool"
EXIFTOOL_REQUEST = EXIF_TOOL + ' -s'
for tag in TAGS_TO_GET:
	EXIFTOOL_REQUEST = EXIFTOOL_REQUEST + ' -' + tag
CHECKSUM_TOOL = "/usr/bin/sha512sum"

class ApoFileError(TypeError):
	def __init__(self):
		self._logger = logging.getLogger('AuPhOrg')

class ApoFileUnknown(ApoFileError):
	def __init__(self, filename):
		super(ApoFileUnknown, self).__init__()
		self.filename = filename
		self.fileext = os.path.splitext(filename)[1][1:]

	def __str__(self):
		self._logger.error('file %s cannot be handled, because its extension (%s) not supported is' % \
			(self.filename, self.fileext))
		return 'file %s cannot be handled, because its extension (%s) not supported is' % \
			(self.filename, self.fileext)

class FilesHandler:
	def __init__(self):
		self._logger = logging.getLogger('AuPhOrg')
		self._logger.info('instanciating DB backend')
		self._db = db_backend.DbConnector()
		self._logger.info('DB backend instanciated')

	def __del__(self):
		self._db = None

	def _file_checksum(self, path):
		'calculates the SHA512 checksum of the file'
		self._logger.info('calculating the checksum of the file %s' % path)
		cmd = CHECKSUM_TOOL + " -b " + path
		output = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout
		lines = output.read().splitlines()
		self._logger.info('checksum of file calculated')
		return lines[0].split(' ')[0]

	def _add_raw_image(self, item_name, path):
		'adds a raw file to the DB'
		self._logger.info('adding the RAW file %s to the item %s' % (path, item_name))
		# calculate the checksums of the file
		file_checksum = self._file_checksum(path)
		# get the timestamp of the last modification
		file_time = os.path.getmtime(path)
		# add the file to the DB and to the item
		self._db.add_raw_file(path, file_time, file_checksum)
		self._db.add_item_content(item_name, path)
		self._logger.info('RAW file added to item')

	def _image_checksum(self, path):
		'calculates the SHA512 checksum of the contained image'
		self._logger.info('calculating the checksum of the image contained in file %s' % path)
		try:
			img = Image.open(path)
			cksm = hashlib.sha512()
			cksm.update(img.tostring())
			self._logger.info('checksum of image calculated')
			return cksm.hexdigest()
		except Exception, err:
			print "Error gettig image from file %s: %s" % (path, str(err))

	def _add_jpeg(self, item_name, path):
		'adds a JPEG file to the DB'
		self._logger.info('adding the JPEG file %s to the item %s' % (path, item_name))
		# get the tags of the file
		exif_tags = {}
		cmd = EXIFTOOL_REQUEST + ' "' + path + '"'
		output = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout
		lines = output.read().splitlines()
		for line in lines:
			(tag_name, _, tag_value) = line.partition(':')
			exif_tags[unicode(tag_name.strip(), 'iso-8859-15')] = unicode(tag_value.strip(), 'iso-8859-15')
		# calculate the checksums of the file
		file_checksum = self._file_checksum(path)
		content_checksum = self._image_checksum(path)
		# get the timestamp of the last modification
		file_time = os.path.getmtime(path)
		# add the file to the DB and to the item
		self._db.add_rich_file(path, file_time, file_checksum, content_checksum, exif_tags)
		self._logger.info('JPEG file added to item')

	def _add_image(self, item_name, path):
		'adds a TIFF file to the DB'
		self._logger.info('adding the TIFF file %s to the item %s' % (path, item_name))
		# calculate the checksums of the file
		file_checksum = self._file_checksum(path)
		content_checksum = self._image_checksum(path)
		# get the timestamp of the last modification
		file_time = os.path.getmtime(path)
		# add the file to the DB
		self._db.add_non_raw_file(path, file_time, file_checksum, content_checksum)
		self._logger.info('TIFF file added to item')

	def _video_checksum(self, path):
		'calculates the checksum of a video, using ffmpeg and md5sum'
		self._logger.info('calculating the checksum of the video contained in file %s' % path)
		cmd = '/usr/bin/ffmpeg -i "' + path + '" -f avi - 2> /dev/null | /usr/bin/sha512sum -b'
		output = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout
		result = output.read().splitlines()[0]
		self._logger.info('checksum of video calculated')
		return result.split(' ')[0]

	def _add_video(self, item_name, path):
		'adds a video file to the DB'
		self._logger.info('adding the video file %s to the item %s' % (path, item_name))
		# calculate the checksums of the file
		file_checksum = self._file_checksum(path)
		content_checksum = self._video_checksum(path)
		# get the timestamp of the last modification
		file_time = os.path.getmtime(path)
		# add the file to the DB
		self._db.add_non_raw_file(path, file_time, file_checksum, content_checksum)
		self._logger.info('video file added to item')

	def _wav_checksum(self, path):
		'calculates the checksum of a wave file'
		self._logger.info('calculating the checksum of the audio contained in file %s' % path)
		try:
			wav = wave.open(path, 'rb')
			cksm = hashlib.sha512()
			cksm.update(wav.readframes(wav.getnframes()))
			self._logger.info('checksum of audio calculated')
			return cksm.hexdigest()
		except Exception, err:
			print "Error getting audio from wave file %s: %s" % (path, str(err))

	def _add_audio(self, item_name, path):
		'adds a video file to the DB'
		self._logger.info('adding the audio file %s to the item %s' % (path, item_name))
		# calculate the checksums of the file
		file_checksum = self._file_checksum(path)
		content_checksum = self._wav_checksum(path)
		# get the timestamp of the last modification
		file_time = os.path.getmtime(path)
		# add the file to the DB
		self._db.add_non_raw_file(path, file_time, file_checksum, content_checksum)
		self._logger.info('audio file added to item')

	def is_older(self, path):
		pass

	def add_file(self, path):
		'adds the file of the given path to the DB'
		self._logger.info('adding the file %s' % path)
		# test that path is file
		if not os.path.isfile(path):
			raise RuntimeError, "path isn't a file!"
		# get file extension to find out type of file
		(item_name, extension) = os.path.splitext(path)
		extension = extension.lower()
		# check if the item already exists
		item = self._db.get_item(item_name)
		# if item doesn't exist, create a new item for the file
		if (item == None):
			self._db.add_item(item_name)
		# if JPEG file
		if (extension in ('.jpg', '.jpeg', '.thm', '.jpe', '.jpg_original')):
			self._add_jpeg(item_name, path)
		elif (extension in ('.avi', '.mov', '.wmv')):
			self._add_video(item_name, path)
		elif (extension in ('.raw', '.rw2')):
			self._add_raw_image(item_name, path)
		elif (extension in ('.tif')):
			self._add_image(item_name, path)
		elif (extension in ('.wav')):
			self._add_audio(item_name, path)
		else:
			raise ApoFileUnknown(path)
		self._logger.info('file added')
