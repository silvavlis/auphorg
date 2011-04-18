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
# path to search the fotos for
PHOTOS_ROOT = '/home/silvavlis/Fotos'

# CLASS photo_file
class PhotoFile:
	'stores the information of a photo file'

	def __init__(self, filepath):
		self.filepath = filepath
		self.exif_tags = {}
		self._get_photo_info()

	def _get_photo_info(self):
		import subprocess

		cmd = EXIFTOOL_REQUEST + ' "' + self.filepath + '"'
		output = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout
		lines = output.read().splitlines()
		for line in lines:
			(tag_name, _, tag_value) = line.partition(':')
			self.exif_tags[tag_name.strip()] = tag_value.strip()

class PhotosCollection:
	tags_to_ignore = ['Subject', 'TagsList', 'Keywords', 'HierarchicalSubject']

	def __init__(self):
		self._photos = []
		for tag in TAGS_TO_GET:
			setattr(self, tag, {})

	def add(self, photo):
		self._photos.append(photo)
		for tag in TAGS_TO_GET:
			if (tag in photo.exif_tags.keys()) and (tag not in PhotosCollection.tags_to_ignore):
				value_tag_photo = photo.exif_tags[tag]
				tag_values = getattr(self,tag)
				if value_tag_photo in tag_values.keys():
					print 'Another photo has already ' + tag + ' = ' + value_tag_photo
					tag_values[value_tag_photo].append(photo)
				else:
					tag_values[value_tag_photo] = [photo]

	def show_dups(self, tag):
		dups = []
		tag_values = getattr(self,tag)
		for tag_value in tag_values.keys():
			if len(tag_values[tag_value]) > 1:
				dups.append(tag_value)
		return dups

class TreeProcessor:
	def __init__(self):
		self.photos = PhotosCollection()
		self._process_tree()

	def _is_photo(self, ext):
		if (ext[1:].lower() in ('jpg', 'jpeg', 'raw')):
			return True
		else:
			return False

	def _process_dir(self, arg, dir_path, filenames):
		import os.path

		for filename in filenames:
			if (self._is_photo(os.path.splitext(filename)[1])):
				photo = PhotoFile(os.path.join(dir_path,filename))
				self.photos.add(photo)

	def _process_tree(self):
		import os.path

		os.path.walk(PHOTOS_ROOT, self._process_dir, None)

# OLD!!!

def get_photo_date_exiftool(file_path):
	import subprocess

	output = subprocess.Popen(GET_DATE + '"' + file_path + '"', shell=True, stdout=subprocess.PIPE).stdout
	date = output.read().partition(':')[2]
	if (date in date_unique.keys()):
		if (date in date_dupl.keys()):
			dups = date_dupl[date]
		else:
			dups = []
			dups.append(date_unique[date])
		dups.append(file_path)
		date_dupl[date] = dups
		print 'date_dupl size = ' + str(len(date_dupl.keys()))
	else:
		date_unique[date] = file_path
#	print 'date_unique size = ' + str(len(date_unique.keys()))

def get_photo_date_pil(file_path):
	import Image, ExifTags

	img = Image.open(file_path)
	info = img._getexif()
	tags = {}
	print info
	import sys
	sys.exit()
	for tag, value in info.items():
		decoded = ExifTags.TAGS.get(tag)
		tags[decoded] = value
	print tags["DateTimeOriginal"]

def main():
	import pickle

	tree = TreeProcessor()
	photos_file = '/tmp/photos.txt'
	fd = open(photos_file, fd)
	pickle.dump(tree.photos,fd)
	fd.close()

if (__name__ == '__main__'):
	main()
