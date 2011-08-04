'handles the database where the multimedia items are stored'

import os
import sqlite3
import re
import inspect
import logging

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
									'timestamp TEXT, ' + \
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
									'tf.path AS tags_file ' + \
									'FROM simple_items i, files cf, files tf ' + \
									'WHERE i.content_file = cf.file_id AND ' + \
									'i.tags_file = tf.file_id;'
SCHEMA_EXTRA_FILES_VIEW = 'CREATE VIEW items_extra_files AS ' + \
									'SELECT i.name AS name, ' + \
									'group_concat(ef.path,"|") AS extra_files ' + \
									'FROM simple_items i, files ef, other_files of ' + \
									'WHERE i.item_id = of.item AND of.file = ef.file_id;'

# exceptions
class ApoDbError(ValueError):
	def __init__(self):
		self._logger = logging.getLogger('AuPhOrg')

class ApoDbDupUniq(ApoDbError):
	def __init__(self, item_type, field, value):
		super(ApoDbDupUniq, self).__init__()
		self.item_type = item_type
		self.field = field
		self.value = value

	def __str__(self):
		err_msg = 'there is already a "%s" with "%s" = "%s"' % \
			(self.item_type, self.field, self.value)
		self._logger.error(err_msg)
		return err_msg

class ApoDbMissingFile(ApoDbError):
	def __init__(self, missing_file, name):
		super(ApoDbMissingFile, self).__init__()
		self.missing_file = missing_file
		self.name = name

	def __str__(self):
		err_msg = 'trying to associate the non-existing file "%s" to the item "%s"!' % \
			(self.missing_file, self.name)
		self._logger.error(err_msg)
		return err_msg

class ApoDbMissingTags(ApoDbError):
	def __init__(self, path):
		super(ApoDbMissingTags, self).__init__()
		self.path = path

	def __str__(self):
		err_msg = 'trying to get the tags of the file "%s"!' % \
			(self.path)
		self._logger.error(err_msg)
		return err_msg

class ApoDbContentExists(ApoDbError):
	def __init__(self, content_file, name):
		super(ApoDbContentExists, self).__init__()
		self.content_file = content_file
		self.name = name

	def __str__(self):
		err_msg = 'trying to associate the content file "%s" to the item "%s", but it already has one!' % \
			(self.content_file, self.name)
		self._logger.error(err_msg)
		return err_msg

#
class DbConnector:
	def __init__(self, db_path=''):
		'connects to DB for saving collection'
		self._logger = logging.getLogger('AuPhOrg')
		if db_path == '':
			if os.path.exists(DB_PATH_TEST):
				self._logger.warning('test DB already exists in %s, removing it' % DB_PATH_TEST)
				os.remove(DB_PATH_TEST)
				self._logger.warning('test DB removed')
			db_path = DB_PATH_TEST
		self._logger.info('connecting to DB %s' % db_path)
		self._db_path = db_path
		self._photos_db = sqlite3.connect(self._db_path)
		self._db_curs = self._photos_db.cursor()
		self._logger.info('connection to DB established')
		# if database doesn't have the tables yet, create them
		self._logger.info('adding the tables to the DB')
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
			self._db_curs.execute(SCHEMA_EXTRA_FILES_VIEW)
			self._photos_db.commit()
			self._db_curs.execute('INSERT INTO files ("path") VALUES (NULL);');
			self.no_file_id = self._db_curs.lastrowid
			self._photos_db.commit()
		self._logger.info('tables added to the DB')

	def __del__(self):
		'closes the connection to the DB before destroying the object'
		self._logger.info('closing connection with DB')
		self._photos_db.close()
		self._logger.info('connection with DB closed')

	def _insert_query(self, table, values):
		self._logger.info('adding an entry to table %s with values %s' % (table, str(values)))
		field_names = ''
		field_placeholders = ''
		query_values = []
		for field in values.keys():
			field_names += field + ', '
			field_placeholders += '?, '
			query_values.append(values[field])
		field_names = field_names[:-2]
		field_placeholders = field_placeholders[:-2]
		sql_query = 'INSERT INTO %s (%s) VALUES (%s);' % \
			(table, field_names, field_placeholders)
		self._logger.info('entry to table %s successfully added' % table)
		return (sql_query, query_values)

	def _update_query(self, table, values, element_filters):
		self._logger.info('updating entry of table %s that matches filter %s with values %s' \
			% (table, str(element_filters), str(values)))
		field_names = ''
		field_placeholders = ''
		query_values = []
		for field in values.keys():
			field_names += field
			field_names += ' = ?, '
			query_values.append(values[field])
		filter_names = ''
		for field_filter in element_filters.keys():
			filter_names += field_filter + ' = ?, '
			query_values.append(element_filters[field_filter])
		filter_names = filter_names[:-2]
		field_names = field_names[:-2]
		sql_query = 'UPDATE %s SET %s WHERE %s;' % \
			(table, field_names, filter_names)
		self._logger.info('entry to table %s successfully updated' % table)
		return (sql_query, query_values)

	def _edit_element(self, table, values, element_filters = None):	
		'inserts the specified values in table, explicitly reporting duplications'
		self._logger.info('editing entry of table %s' % table)
		if (element_filters == None):
			(sql_query, query_values) = self._insert_query(table, values)
		else:
			(sql_query, query_values) = self._update_query(table, values, element_filters)
		try:
			self._db_curs.execute(sql_query, query_values)
			self._photos_db.commit()
		except sqlite3.IntegrityError, err:
			dup_field = re.match(r'column (.*) is not unique', str(err))
			if (dup_field == None):
				raise
			else:
				err_field = dup_field.group(1)
				item_type = table[:-1]
				raise ApoDbDupUniq(item_type, err_field, values[err_field])
		self._logger.info('done editing entry')
		return self._db_curs.lastrowid

	def add_non_raw_file(self, path, timestamp, file_checksum, content_checksum):
		'adds a file without metadata to the DB'
		self._logger.info('adding non RAW file %s', path)
		self._edit_element('files', {\
			'path': path, \
			'timestamp': timestamp, \
			'file_checksum': file_checksum, \
			'content_checksum': content_checksum})
		self._logger.info('non RAW file added')

	def add_raw_file(self, path, timestamp, file_checksum):
		'adds a file without metadata to the DB'
		self._logger.info('adding RAW file %s', path)
		self._edit_element('files', {\
			'path': path, \
			'timestamp': timestamp, \
			'file_checksum': file_checksum})
		self._logger.info('RAW file added')

	def add_tags(self, model = '', software = '', date_time_original = '',
						create_date = '', image_width = '', image_height = '',
						tags_list = '',	hierarchical_subject = '',
						subject = '', keywords = ''):
		'adds a tags item to the DB'
		self._logger.info('adding item tags')
		rowid = self._edit_element('tags', { \
			'Model': model, \
			'Software': software, \
			'DateTimeOriginal': date_time_original, \
			'CreateDate': create_date, \
			'ImageWidth': image_width, \
			'ImageHeight': image_height, \
			'TagsList': tags_list, \
			'HierarchicalSubject': hierarchical_subject, \
			'Subject': subject, \
			'Keywords': keywords})
		self._logger.info('item tags successfully added')
		return rowid

	def add_rich_file(self, path, timestamp, file_checksum, image_checksum, tags):
		'adds a file with metadata to the DB'
		self._logger.info('adding rich file %s', path)
		tags_index = self.add_tags(tags['Model'], tags['Software'], 
										tags['DateTimeOriginal'], tags['CreateDate'], 
										tags['ImageWidth'], tags['ImageHeight'], 
										tags['TagsList'], tags['HierarchicalSubject'], 
										tags['Subject'], tags['Keywords'])
		self._edit_element('files', { \
			'path': path, \
			'timestamp': timestamp, \
			'file_checksum': file_checksum, \
			'content_checksum': image_checksum, \
			'tags': tags_index})
		self._logger.info('rich file added')

	def get_rich_file_tags(self, path):
		'gets the tags of a rich file'
		self._logger.info('getting tags of rich file %s', path)
		self._db_curs.execute('SELECT t.* FROM tags t, files f WHERE t.tags_id = f.tags AND path = ?;', [path])
		tags_list = self._db_curs.fetchone()
		if tags_list == None:
			raise ApoDbMissingTags(path)
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
		self._logger.info('tags of rich file obtained')
		return tags

	def add_item(self, name):
		'adds a multimedia item to the DB'
		self._logger.info('adding item %s' % name)
		self._edit_element('simple_items', {'name': name, 'content_file': self.no_file_id, 'tags_file': self.no_file_id})
		self._logger.info('item added')

	def add_item_content(self, name, content_file):
		'adds a content file to a multimedia item into the DB'
		self._logger.info('adding to item %s the file %s as its content file' % (name, content_file))
		# check that the item doesn't contain any content file yet
		(_, cf, tf, _) = self.get_item(name)
		if (cf != None) and (cf != tf):
			raise ApoDbContentExists(content_file, name)
		# add the content file to the item
		cur = self._db_curs
		cur.execute('SELECT file_id FROM files WHERE path = "%s";' % content_file)
		try:
			content_file_id = cur.fetchone()[0]
		except TypeError:
			raise ApoDbMissingFile(content_file, item_name)
		self._edit_element('simple_items', {'content_file': content_file_id}, {'name': name})
		self._logger.info('item added')

	def add_item_tags(self, name, tags_file):
		'adds a tags file to a multimedia item into the DB'
		self._logger.info('adding to item %s the file %s as its metadata file' % (name, tags_file))
		cur = self._db_curs
		cur.execute('SELECT file_id FROM files WHERE path = "%s";' % tags_file)
		try:
			tags_file_id = cur.fetchone()[0]
		except TypeError:
			raise ApoDbMissingFile(tags_file, item_name)
		self._edit_element('simple_items', {'tags_file': tags_file_id}, {'name': name})
		self._logger.info('item added')

	def get_item(self, item_name):
		'returns the specified item'
		self._logger.info('getting item %s' % item_name)
		self._db_curs.execute('SELECT * FROM items WHERE name = ?;', [item_name])
		try:
			(item_name, content_file, tags_file) = self._db_curs.fetchone()
		except TypeError, err:
			if (str(err) == "'NoneType' object is not iterable"):
				self._logger.warning("item %s doesn't exist yet" % item_name)
				return None
			else:
				raise
		if (content_file == None) and (tags_file != None):
			content_file = tags_file
		self._db_curs.execute('SELECT * FROM items_extra_files WHERE name = ?;', [item_name])
		extra_files = self._db_curs.fetchone()
		if extra_files != None:
			extra_files = extra_files[1].split('|')
		self._logger.info('item obtained')
		return (item_name, content_file, tags_file, extra_files)

	def add_extra_file(self, file_path, item_name):
		'adds a relationship with an extra file to the DB'
		self._logger.info('extending item %s by adding the extra file %s' % (item_name, file_path))
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
		self._edit_element('other_files', { \
			'file': file_id, \
			'item': item_id})
		self._logger.info('item extended with extra file')
