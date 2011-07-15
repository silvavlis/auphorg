'handles the database where the multimedia items are stored'

import os
import sqlite3
import re
import inspect

DB_PATH_TEST = os.path.join('/tmp/test_auphorg.db')

#DB schema
SCHEMA_TAGS = 'CREATE TABLE tags (' + \
									'tags_id INTEGER PRIMARY KEY ASC, ' + \
									'Model TEXT, ' + \
									'Software TEXT, ' + \
									'DateTimeOriginal TEXT, ' + \
									'CreateDate TEXT, ' + \
									'ImageWidth TEXT, ' + \
									'ImageHeight TEXT, ' + \
									'TagsList TEXT, ' + \
									'HierarchicalSubject TEXT, ' + \
									'Subject TEXT, ' + \
									'Keywords TEXT);'
SCHEMA_FILES = 'CREATE TABLE files (' + \
									'file_id INTEGER PRIMARY KEY ASC, ' + \
									'path TEXT UNIQUE, ' + \
									'file_checksum TEXT, ' + \
									'content_checksum TEXT, ' + \
									'tags REFERENCES tags(tags_id));'
SCHEMA_ITEMS = 'CREATE TABLE simple_items (' + \
									'item_id INTEGER PRIMARY KEY ASC, ' + \
									'name TEXT UNIQUE, ' + \
									'content_file REFERENCES files(file_id) UNIQUE, ' + \
									'tags_file REFERENCES files(file_id) UNIQUE);'
SCHEMA_OTHER_FILES = 'CREATE TABLE other_files(' + \
									'file REFERENCES files(file_id), ' + \
									'item REFERENCES simple_items(item_id));'
SCHEMA_FILE_INDEX = 'CREATE INDEX file_path ON files(path);'
SCHEMA_ITEM_INDEX = 'CREATE INDEX item_name ON simple_items(name);'
SCHEMA_ITEMS_VIEW = 'CREATE VIEW items AS ' + \
									'SELECT i.name AS name, ' + \
									'cf.path AS content_file, ' + \
									'tf.path AS tags_file, ' + \
									'group_concat(ef.path,"|") AS extra_files ' + \
									'FROM simple_items i, files cf, files tf, files ef, other_files of ' + \
									'WHERE i.content_file = cf.file_id AND ' + \
									'i.tags_file = tf.file_id AND ' + \
									'i.item_id = of.item AND ' + \
									'of.file = ef.file_id;'

# exceptions
class ApoDbError(ValueError):
	pass

class ApoDbDupUniq(ApoDbError):
	def __init__(self, item_type, field, value):
		self.item_type = item_type
		self.field = field
		self.value = value

	def __str__(self):
		return 'there is already a %s with %s = %s' % \
			(self.item_type, self.field, self.value)

class ApoDbMissingFile(ApoDbError):
	def __init__(self, missing_file, name):
		self.missing_file = missing_file
		self.name = name

	def __str__(self):
		return 'trying to associate the non-existing file %s to the item %s!' % \
			(self.missing_file, self.name)

#
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
		if (not (u'files',) in tables) or (not (u'simple_items',) in tables) or (not (u'tags',) in tables):
			self._db_curs.execute(SCHEMA_TAGS)
			self._db_curs.execute(SCHEMA_FILES)
			self._db_curs.execute(SCHEMA_ITEMS)
			self._db_curs.execute(SCHEMA_OTHER_FILES)
			self._db_curs.execute(SCHEMA_FILE_INDEX)
			self._db_curs.execute(SCHEMA_ITEM_INDEX)
			self._db_curs.execute(SCHEMA_ITEMS_VIEW)
			self._photos_db.commit()

	def __del__(self):
		'closes the connection to the DB before destroying the object'
		self._photos_db.close()

	def _insert_element(self, table, values):	
		'inserts the specified values in table, explicitly reporting duplications'
		field_names = ''
		field_placeholders = ''
		field_values = []
		for field in values.keys():
			field_names += field + ', '
			field_placeholders += '?, '
			field_values.append(values[field])
		field_names = field_names[:-2]
		field_placeholders = field_placeholders[:-2]
		sql_insert = 'INSERT INTO %s (%s) VALUES (%s);' % \
			(table, field_names, field_placeholders)
		try:
			self._db_curs.execute(sql_insert, field_values)
			self._photos_db.commit()
		except sqlite3.IntegrityError, err:
			dup_field = re.match(r'column (.*) is not unique', str(err))
			if (dup_field == None):
				raise
			else:
				err_field = dup_field.group(1)
				item_type = table[:-1]
				raise ApoDbDupUniq(item_type, err_field, values[err_field])
		return self._db_curs.lastrowid

	def add_non_raw_file(self, path, file_checksum, content_checksum):
		'adds a file without metadata to the DB'
		values = {}
		values['path'] = path
		values['file_checksum'] = file_checksum
		values['content_checksum'] = content_checksum
		self._insert_element('files', values)

	def add_raw_file(self, path, file_checksum):
		'adds a file without metadata to the DB'
		values = {}
		values['path'] = path
		values['file_checksum'] = file_checksum
		self._insert_element('files', values)

	def add_tags(self, model = '', software = '', date_time_original = '',
						create_date = '', image_width = '', image_height = '',
						tags_list = '',	hierarchical_subject = '',
						subject = '', keywords = ''):
		'adds a tags item to the DB'
		values = {}
		values['Model'] = model
		values['Software'] = software
		values['DateTimeOriginal'] = date_time_original
		values['CreateDate'] = create_date
		values['ImageWidth'] = image_width
		values['ImageHeight'] = image_height
		values['TagsList'] = tags_list
		values['HierarchicalSubject'] = hierarchical_subject
		values['Subject'] = subject
		values['Keywords'] = keywords
		return self._insert_element('tags', values)

	def add_rich_file(self, path, file_checksum, image_checksum, tags):
		'adds a file with metadata to the DB'
		tags_index = self.add_tags(tags['Model'], tags['Software'], 
										tags['DateTimeOriginal'], tags['CreateDate'], 
										tags['ImageWidth'], tags['ImageHeight'], 
										tags['TagsList'], tags['HierarchicalSubject'], 
										tags['Subject'], tags['Keywords'])
		values = {}
		values['path'] = path
		values['file_checksum'] = file_checksum
		values['content_checksum'] = image_checksum
		values['tags'] = tags_index
		self._insert_element('files', values)

	def get_rich_file_tags(self, path):
		self._db_curs.execute('SELECT t.* FROM tags t, files f WHERE t.tags_id = f.tags AND path = ?;', [path])
		tags_list = self._db_curs.fetchone()
		tags = {}
		tags['Model'] = tags_list[1]
		tags['Software'] = tags_list[2]
		tags['DateTimeOriginal'] = tags_list[3]
		tags['CreateDate'] = tags_list[4]
		tags['ImageWidth'] = tags_list[5]
		tags['ImageHeight'] = tags_list[6]
		tags['TagsList'] = tags_list[7]
		tags['HierarchicalSubject'] = tags_list[8]
		tags['Subject'] = tags_list[9]
		tags['Keywords'] = tags_list[10]
		return tags

	def add_item(self, name):
		'adds a multimedia item to the DB'
		values = {}
		values['name'] = name
		self._insert_element('simple_items', values)

	def add_item_content(self, name, content_file):
		'adds a content file to a multimedia item into the DB'
		cur = self._db_curs
		cur.execute('SELECT file_id FROM files WHERE path = "%s";' % content_file)
		try:
			content_file_id = cur.fetchone()[0]
		except TypeError:
			raise ApoDbMissingFile(content_file, item_name)
		cur.execute('SELECT content_file FROM items WHERE name = ?;', (name,))
		self._db_curs.execute('UPDATE simple_items SET content_file = ? WHERE name = ?;', (content_file_id, name))
		self._photos_db.commit()
		return self._db_curs.lastrowid

	def add_item_tags(self, name, tags_file):
		'adds a tags file to a multimedia item into the DB'
		cur = self._db_curs
		cur.execute('SELECT file_id FROM files WHERE path = "%s";' % tags_file)
		try:
			tags_file_id = cur.fetchone()[0]
		except TypeError:
			raise ApoDbMissingFile(tags_file, item_name)
		self._db_curs.execute('UPDATE simple_items SET tags_file = ? WHERE name = ?;', (tags_file_id, name))
		self._photos_db.commit()
		return self._db_curs.lastrowid

	def get_item(self, item_name):
		'returns the specified item'
		self._db_curs.execute('SELECT * FROM items WHERE name = ?;', [item_name])
		try:
			(item_name, content_file, tags_file, extra_files) = self._db_curs.fetchone()
		except TypeError, err:
			if (str(err) == "'NoneType' object is not iterable"):
				return None
			else:
				raise
		extra_files = extra_files.split('|')
		return (item_name, content_file, tags_file, extra_files)

	def add_extra_file(self, file_path, item_name):
		'adds a relationship with an extra file to the DB'
		cur = self._db_curs
		cur.execute('SELECT file_id FROM files WHERE path = "%s";' % file_path)
		try:
			file_id = cur.fetchone()[0]
		except TypeError:
			raise IndexError, "trying to a associate the unknown file %s to the ' + \
				'item %s in an 'other file' relationship!" % (file_path, item_name)
		cur.execute('SELECT item_id FROM simple_items WHERE name = "%s";' % item_name)
		try:
			item_id = cur.fetchone()[0]
		except TypeError:
			raise IndexError, "trying to a associate the file %s to the unknown ' + \
				'item %s in an 'other file' relationship!" % (file_path, item_name)
		cur.execute('INSERT INTO other_files (file, item) ' + \
						'VALUES (?, ?);', (file_id, item_id))
		self._photos_db.commit()
		return cur.lastrowid
