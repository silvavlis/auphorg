#!/usr/bin/python

# create exiftool request string
EXIF_TOOL = "/usr/bin/exiftool"
TAGS_TO_GET = [ 'FileName',
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
EXIFTOOL_REQUEST = EXIF_TOOL + ' -s'
for tag in TAGS_TO_GET:
	EXIFTOOL_REQUEST = EXIFTOOL_REQUEST + ' -' + tag

class CameraItem:
	def __init__(self, file_paths):
		self.file_paths = file_paths
		self.item_tags = ''
		self.item_checksum = ''
	def _get_item_info(self):
		pass
	def _get_item_checksum(self):
		pass
	def remove(self):
		pass
	def move(self):
		pass

class PhotoFile:
	'stores the information of a photo file'

	def __init__(self, filepath):
		self.filepath = filepath
		self.exif_tags = self._get_photo_info()
		self.image_checksum = self._get_image_checksum()

	def _get_photo_info(self):
		import subprocess

		exif_tags = {}
		cmd = EXIFTOOL_REQUEST + ' "' + self.filepath + '"'
		output = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout
		lines = output.read().splitlines()
		for line in lines:
			(tag_name, _, tag_value) = line.partition(':')
			exif_tags[tag_name.strip()] = tag_value.strip()
		return exif_tags

	def _get_image_checksum(self):
		import hashlib, Image

		try:
			img = Image.open(self.filepath)
			cksm = hashlib.sha512()
			cksm.update(img.tostring())
			checksum = cksm.digest()
		except Exception, err:
			print "Error gettig image from file %s: %s" % (self.filepath, str(err))
		return checksum

class PhotosCollection:
	tags_to_ignore = ['Subject', 'TagsList', 'Keywords', 'HierarchicalSubject']

	def __init__(self):
		self._photos = []
		for tag in TAGS_TO_GET:
			setattr(self, tag, {})
		self.image_checksums = {}

	def add(self, photo):
		self._photos.append(photo)
		for tag in TAGS_TO_GET:
			if (tag in photo.exif_tags.keys()) and (tag not in PhotosCollection.tags_to_ignore):
				value_tag_photo = photo.exif_tags[tag]
				tag_values = getattr(self,tag)
				if value_tag_photo in tag_values.keys():
					tag_values[value_tag_photo].append(photo)
				else:
					tag_values[value_tag_photo] = [photo]
		if photo.image_checksum in self.image_checksums.keys():
			self.image_checksums[photo.image_checksum].append(photo)
		else:
			self.image_checksums[photo.image_checksum] = [photo]

	def show_dup_tags(self, tag):
		dups = []
		tag_values = getattr(self,tag)
		for tag_value in tag_values.keys():
			if len(tag_values[tag_value]) > 1:
				dups.append(tag_value)
		return dups

	def show_dup_cksms(self):
		dups = []
		for checksum in self.image_checksums.keys():
			if len(self.image_checksums) > 1:
				print checksum
				dups.append(checksum)

class TreeProcessor:
	def __init__(self):
		self.photos = PhotosCollection()
		self.jpegs = []
		self.raws = []
		self.sidecars = []

	def _is_jpeg(self, ext):
		if (ext[1:].lower() in ('jpg', 'jpeg')):
			return True
		else:
			return False

	def _is_raw(self, ext):
		if (ext[1:].lower() in ('raw')):
			return True
		else:
			return False

	def _is_sidecar(self, filepath):
		import glob, os.path
		same_name = glob.glob(os.path.splitext(filepath)[0] + '.*')
		if len(same_name) > 1:
			return True
		else:
			return False

	def _process_dir(self, arg, dir_path, filenames):
		import os.path

		for filename in filenames:
			filepath = os.path.join(dir_path, filename)
			if not os.path.isdir(filepath):
				if self._is_jpeg(os.path.splitext(filename)[1]):
					if self._is_sidecar(filepath):
						self.sidecars.append(filepath)
					else:
						self.jpegs.append(filepath)
				if self._is_raw(os.path.splitext(filename)[1]):
					self.raws.append(filepath)

	def scan_tree(self, photostree_root):
		import os.path
		os.path.walk(photostree_root, self._process_dir, None)

	def process_tree(self):
		n_jpeg_files = len(self.jpegs)
		i = 0
		for jpeg_file in self.jpegs:
			photo = PhotoFile(jpeg_file)
			self.photos.add(photo)
			i = i + 1
			if (i % (n_jpeg_files/10)) == 0:
				print "%d%% already processed" % (i*100/ n_jpeg_files)

def scan_tree(photos_file):
	print "No valid photos information file found, scanning tree"
	import pickle
	photostree_root = '/home/silvavlis/Fotos'
	fd = open(photos_file,'wb')
	tree = TreeProcessor()
	tree.scan_tree(photostree_root)
	print "Tree analyzed: %d jpegs found, %d raws found and %d sidecars found." % (len(tree.jpegs), len(tree.raws), len(tree.sidecars))
	tree.process_tree()
	print "Tree processed, dumping information"
	pickle.dump(tree.photos,fd)
	fd.close()

def analyze_tree(photos_file):
	print "Photos information file found, analyzing tree"
	import pickle
	tree = pickle.load(photos_file)
	print "Photos information loaded, processing duplicates"
	print tree.show_dup_cksms()

if (__name__ == '__main__'):
	import os.path
	photos_file = '/home/silvavlis/program_org_fotos/photos_info.dat'
	if (os.path.exists(photos_file)) and (os.path.getsize(photos_file) > 0):
		analyze_tree(photos_file)
	else:
		scan_tree(photos_file)
