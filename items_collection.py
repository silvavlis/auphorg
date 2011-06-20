'''
Handles the identified CameraItem's.
'''

import os
import datetime
import sys

# create exiftool request string
TAGS_CLASSIFICATION = [
				'FileName',
				'Model',
				'Software',
				'DateTimeOriginal',
				'CreateDate',
				'ImageWidth',
				'ImageHeight'
				]

# collection of items
class ItemsCollection(dict):
	'handles the identified CameraItems'
	# private methods
	def __init__(self, files_to_process, *args):
		'initializes the collection'
		dict.__init__(self, args)
		for tag in TAGS_TO_GET:
			setattr(self, tag, {})
		self.content_checksum = {}
		self.thumbnail_checksum = {}
		# add the files to the items
		n_files_to_process = len(files_to_process)
		i = 0
		for file in files_to_process:
			self._add(file)
			i = i + 1
			if (i % (n_files_to_process/10)) == 0:
				trace("%d%% already processed" % ((i*100/ n_files_to_process) + 1))
		# connect to database for saving collection
		self._connect_db()

	def _connect_db(self, db_path=''):
		'connect to database for saving collection'
		if path == '':
			db_path = os.path.join(os.getcwd, 'photos_db.dat')
		self._photos_db = sqlite3.connect(db_path)
		self._db_curs = self._photos_db.cursor()
		# if database doesn't exist yet, creates it
		if not os.path.exists(db_path):
			self._db_curs().execute('CREATE TABLE files (' + \
										'path TEXT PRIMARY KEY ASC, ' + \
										'checksum TEXT), ' + \
										'FOREIGN KEY(container) REFERENCES items(name)')
			self._db_curs().execute('CREATE TABLE items (' + \
										'name TEXT PRIMARY KEY ASC, ' + \
										'FOREIGN KEY(content_file) REFERENCES files(path), ' + \
										'FOREIGN KEY(tags_file) REFERENCES files(path)), ' + \
										'has_sidecars BOOLEAN DEFAULT false')
			self._photos_db.commit()
	
	def _get_item(self, item_name):
		self._db_curs().execute('SELECT * FROM items WHERE name=?', (item_name,))
		result = self._db_curs().fetchone()
		return CameraItem(item_name, content_file = result[1], tags_file = result[2])

	def _add(self, file_path):
		'adds item to the list'
		# get the name of the item
		item_name = CameraItem.path_to_name(file_path)
		if item_name == '':
			trace_err('WARNING! trying to create an item with empty name?!')
			return
		# if the item already exists, get the item
		item_attrs = self._get_item(item_name)
		if item_attrs == None:
			item = CameraItem(item_name)(?,?,?)', (?,?,?)', 
			self._db_curs().execute('INSERT INTO items VALUES (?,?,?)', (item_name, '', ''))
		else:
			content_file = item_attrs[1]
			tags_file = item_attrs[2]
			item = CameraItem(item_name, content_file, tags_file)
		# add the file to the item
		item.add(file_path)
		# example for updating file 'content_file' of item
		self._db_curs().execute('UPDATE items SET content_file=? WHERE name=?', (content_file, item_name))

	def _add_old(self, file_path):
		'adds item to the list'
		# get the name of the item
		item_name = CameraItem.path_to_name(file_path)
		if item_name == '':
			return
		# if the item already exists, get the item
		if item_name in self.keys():
			item = self[item_name]
		# if the item doesn't exist yet, create a new one and add it to the list
		else:
			item = CameraItem(item_name)
			self[item_name] = item
		# add the file to the item
		item.add(file_path)

	# public methods
	def generate_dicts(self):
		'generate the dictionaries containing items with the same tags or checksums'
		i = 0
		for item_name in self.keys():
			item = self[item_name]
			for tag in TAGS_TO_GET:
				item_tag_value = getattr(item, tag)
				known_tag_values = getattr(self, tag)
				if (item_tag_value != '') and (tag not in ItemsCollection.TAGS_CLASSIFICATION):
					if item_tag_value in known_tag_values.keys():
						known_tag_values[item_tag_value].append(item)
					else:
						known_tag_values[item_tag_value] = [item]
			if item.content_checksum in self.content_checksum.keys():
				self.content_checksum[item.content_checksum].append(item)
			else:
				self.content_checksum[item.content_checksum] = [item]
			if item.thumbnail_checksum in self.thumbnail_checksum.keys():
				self.thumbnail_checksum[item.thumbnail_checksum].append(item)
			else:
				self.thumbnail_checksum[item.thumbnail_checksum] = [item]
			i = i + 1
			sys.stdout.write('.')
			sys.stdout.flush()
			if (i % (len(self.keys())/10)) == 0:
				trace("\n%d%% already processed" % ((i*100/ len(self.keys())) + 1))

	def show_dup_tags(self, tag):
		dups = []
		tag_values = getattr(self,tag)
		for tag_value in tag_values.keys():
			if len(tag_values[tag_value]) > 1:
				dups.append(tag_value)
		return dups

	def show_dup_cksms(self):
		dups = []
		for checksum in self.content_checksum.keys():
			if len(self.content_checksum) > 1:
				trace(checksum)
				dups.append(checksum)
