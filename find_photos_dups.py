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

# CLASS photo_file
class PhotoFile:
	'stores the information of a photo file'

	def __init__(self, filepath):
		self.filepath = filepath
		self.exif_tags = self._get_photo_info()
		self.image_checksum = self._get_image_checksum()
#		print self.image_checksum

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
#			import sys
#			print "Error %d getting image from file %s (%s)" % (sys.exc_info()[0], self.filepath
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
#					print photo.filepath + ': photos that have already ' + tag + ' = ' + value_tag_photo + ' =>'
#					print tag_values[value_tag_photo]
					tag_values[value_tag_photo].append(photo)
				else:
					tag_values[value_tag_photo] = [photo]
		if photo.image_checksum in self.image_checksums.keys():
#			print photo.filepath + ': photos with exactly the same image =>'
#			for foto in self.image_checksums[photo.image_checksum]:
#				print '\t' + foto.filepath
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

class TreeAnalyzer:
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
		return False

	def _process_dir(self, arg, dir_path, filenames):
		import os.path

		for filename in filenames:
			if self._is_sidecar(os.path.join(dir_path,filename)):
				print "%s is a sidecar file, it cannot be processed yet :-(" % os.path.join(dir_path,filename)
			else:
				if self._is_jpeg(os.path.splitext(filename)[1]):
					photo = PhotoFile(os.path.join(dir_path,filename))
					self.photos.add(photo)
			if self._is_raw(os.path.splitext(filename)[1]):
				print "%s is a RAW file, it cannot be processed yet :-(" % os.path.join(dir_path,filename)

	def process_tree(self, photostree_root):
		import os.path

		os.path.walk(photostree_root, self._process_dir, None)

def scan_tree(photos_file):
	print "No photos information file found, scanning tree"
	import pickle
	photostree_root = '/home/silvavlis/Fotos'
	fd = open(photos_file,'wb')
	tree = TreeAnalyzer()
	tree.process_tree(photostree_root)
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
	if not os.path.exists(photos_file):
		scan_tree(photos_file)
	else:
		analyze_tree(photos_file)
