'''
'''

import os
import sqlite3

DB_PATH_TEST = os.path.join('/tmp/test_auphorg.db')

#DB schema
SCHEMA_TAGS = 'CREATE TABLE tags (' + \
									'tags_id INTEGER PRIMARY KEY ASC, ' + \
									'model TEXT, ' + \
									'software TEXT, ' + \
									'date_time_original TEXT, ' + \
									'create_date TEXT, ' + \
									'image_width TEXT, ' + \
									'image_height TEXT, ' + \
									'tags_list TEXT, ' + \
									'hierarchical_subject TEXT, ' + \
									'subject TEXT, ' + \
									'keywords TEXT);'
SCHEMA_FILES = 'CREATE TABLE files (' + \
									'file_id INTEGER PRIMARY KEY ASC, ' + \
									'path TEXT, ' + \
									'file_checksum TEXT, ' + \
									'content_checksum TEXT, ' + \
									'tags INTEGER, ' + \
									'FOREIGN KEY(tags) REFERENCES tags(rowid));'
SCHEMA_ITEMS = 'CREATE TABLE items (' + \
									'item_id INTEGER PRIMARY KEY ASC, ' + \
									'name TEXT, ' + \
									'content_file INTEGER, ' + \
									'tags_file INTEGER, ' + \
									'FOREIGN KEY(content_file) REFERENCES files(rowid), ' + \
									'FOREIGN KEY(tags_file) REFERENCES files(rowid));'
SCHEMA_OTHER_FILES = 'CREATE TABLE other_files(' + \
									'file INTEGER PRIMARY KEY ASC, ' + \
									'item INTEGER, ' + \
									'FOREIGN KEY(file) REFERENCES files(rowid), ' + \
									'FOREIGN KEY(item) REFERENCES items(rowid));'
SCHEMA_FILE_INDEX = 'CREATE INDEX file_path ON files(path);'
SCHEMA_ITEM_INDEX = 'CREATE INDEX item_name ON items(name);'

class DbConnector:
	def __init__(self, db_path=''):
		'connects to DB for saving collection'
		if db_path == '':
			db_path = DB_PATH_TEST
		self._db_path = db_path
		self._photos_db = sqlite3.connect(self._db_path)
		self._db_curs = self._photos_db.cursor()
		# if database doesn't have the tables yet, create them
		self._db_curs.execute("SELECT name FROM sqlite_master WHERE type='table';")
		tables = self._db_curs.fetchall()
		if (not (u'files',) in tables) or (not (u'items',) in tables) or (not (u'tags',) in tables):
			self._db_curs.execute(SCHEMA_TAGS)
			self._db_curs.execute(SCHEMA_FILES)
			self._db_curs.execute(SCHEMA_ITEMS)
			self._db_curs.execute(SCHEMA_OTHER_FILES)
			self._db_curs.execute(SCHEMA_FILE_INDEX)
			self._db_curs.execute(SCHEMA_ITEM_INDEX)
			self._photos_db.commit()
	def __del__(self):
		'closes the connection to the DB before destroying the object'
		self._photos_db.close()

	def add_poor_file(self, path, file_checksum, image_checksum):
		'adds a file without metadata to the DB'
		cur = self._db_curs
		cur.execute('INSERT INTO files (path, file_checksum, content_checksum) ' + \
					'VALUES (?, ?, ?);', (path, file_checksum, image_checksum))
		self._photos_db.commit()
		return cur.lastrowid

	def add_tags(self, model = '', software = '', date_time_original = '',
						create_date = '', image_width = '', image_height = '',
						tags_list = '',	hierarchical_subject = '',
						subject = '', keywords = ''):
		'adds a tags item to the DB'
		cur = self._db_curs
		cur.execute('INSERT INTO tags (model, software, date_time_original, ' + \
					'create_date, image_width, image_height, tags_list, ' + \
					'hierarchical_subject, subject, keywords) ' + \
					'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);', 
					(model, software, date_time_original, create_date, 
					image_width, image_height, tags_list, 
					hierarchical_subject, subject, keywords))
		self._photos_db.commit()
		return cur.lastrowid

	def add_rich_file(self, path, file_checksum, image_checksum, tags):
		'adds a file with metadata to the DB'
		cur = self._db_curs
		tags_index = self.add_tags(tags['model'], tags['software'], 
										tags['date_time_original'], tags['create_date'], 
										tags['image_width'], tags['image_height'], 
										tags['tags_list'], tags['hierarchical_subject'], 
										tags['subject'], tags['keywords'])
		cur.execute('INSERT INTO files (path, file_checksum, content_checksum, tags) ' + \
					'VALUES (?, ?, ?, ?);',	(path, file_checksum, image_checksum, tags_index))
		self._photos_db.commit()
		return cur.lastrowid

	def add_item(self, name, content_file, tags_file):
		'adds a multimedia item to the DB'
		cur = self._db_curs
		cur.execute('SELECT file_id FROM files WHERE path = "%s";' % content_file)
		try:
			content_file_id = cur.fetchone()[0]
		except TypeError:
			raise IndexError, "trying to associate the non-existing file %s to the item %s!" % (content_file, name)
		if tags_file != "":
			cur.execute('SELECT file_id FROM files WHERE path = "%s";' % tags_file)
			try:
				tags_file_id = cur.fetchone()[0]
			except TypeError:
				raise IndexError, "trying to associate the non-existing file %s to the item %s!" % (tags_file, name)
		self._db_curs.execute('INSERT INTO items (name, content_file, tags_file) ' + \
								'VALUES (?, ?, ?);',
								(name, content_file_id, tags_file_id))
		self._photos_db.commit()
		return self._db_curs.lastrowid

	def get_item(self, item_name):
		self._db_curs.execute('SELECT * FROM items WHERE name=?', (item_name,))
		result = self._db_curs.fetchone()
		return CameraItem(item_name, content_file = result[1], tags_file = result[2])

	def add_extra_file(self, file_path, item_name):
		'adds a relationship with an extra file to the DB'
		cur = self._db_curs
		cur.execute('SELECT file_id FROM files WHERE path = "%s";' % file_path)
		try:
			file_id = cur.fetchone()[0]
		except TypeError:
			raise IndexError, "trying to a associate the unknown file %s to the item %s in an 'other file' relationship!" % (file_path, item_name)
		cur.execute('SELECT item_id FROM items WHERE name = "%s";' % item_name)
		try:
			item_id = cur.fetchone()[0]
		except TypeError:
			raise IndexError, "trying to a associate the file %s to the unknown item %s in an 'other file' relationship!" % (file_path, item_name)
		cur.execute('INSERT INTO other_files (file, item) ' + \
						'VALUES (?, ?);', (file_id, item_id))
		self._photos_db.commit()
		return cur.lastrowid

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
			item = CameraItem(item_name)
			self._db_curs.execute('INSERT INTO items VALUES (?,?,?)', (item_name, '', ''))
			self._photos_db.commit()
		else:
			content_file = item_attrs[1]
			tags_file = item_attrs[2]
			item = CameraItem(item_name, content_file, tags_file)
		# add the file to the item
		item.add(file_path)
		# example for updating file 'content_file' of item
		self._db_curs.execute('UPDATE items SET content_file=? WHERE name=?', (content_file, item_name))
		self._photos_db.commit()
