#!/usr/bin/python

import unittest
import sqlite3
import os

import db_backend
import files_handler

class TestDbBackend(unittest.TestCase):
	test_tags = {'model': u'mod',
				'software': u'soft',
				'date_time_original': u'dat_org',
				'create_date': u'creat_dat',
				'image_width': u'img_w',
				'image_height': u'img_h',
				'tags_list': u'tags',
				'hierarchical_subject': u'hierc_subj',
				'subject': u'subjs',
				'keywords': u'keyws'
				}
	test_file_poor_1 = {'path': u'/this/is/a/path_1',
						'file_checksum': u'f1l3ch3cksum1',
						'content_checksum': u'c0nt3ntch3cksum1'
						}
	test_file_rich_1 = {'path': u'/this/is/a/path_2',
						'file_checksum': u'f1l3ch3cksum2',
						'content_checksum': u'c0nt3ntch3cksum2',
						'tags': test_tags
						}
	test_file_poor_2 = {'path': u'/this/is/a/path_3',
						'file_checksum': u'f1l3ch3cksum3',
						'content_checksum': u'c0nt3ntch3cksum3'
						}
	test_file_poor_3 = {'path': u'/this/is/a/path_4',
						'file_checksum': u'f1l3ch3cksum4',
						'content_checksum': u'c0nt3ntch3cksum4'
						}
	test_item = {'name': u'/this/is/a/name', 
						'content_file': test_file_poor_1['path'],
						'tags_file': test_file_rich_1['path']
						}
	test_extra_file_1 = {'file': test_file_poor_2['path'],
						'item': test_item['name']
						}
	test_extra_file_2 = {'file': test_file_poor_3['path'],
						'item': test_item['name']
						}

	def setUp(self):
		# create DB connection and get DB path
		self._db = db_backend.DbConnector()	
		self._db_path = self._db._db_path

	def tearDown(self):
		# delete DB
		os.remove(self._db_path)

	def test_db_creation(self):
		'test that the DB exists and has the expected tables'
		# check that the file containing the DB exists
		self.assertTrue(os.path.exists(self._db_path))
		# initialize the DB connection to check the schema
		db = sqlite3.connect(self._db_path)
		cur = db.cursor()
		# check that the expected tables are available
		cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
		tables = cur.fetchall()
		self.assertTrue(len(tables) == 4)
		self.assertTrue((u'simple_items',) in tables)
		self.assertTrue((u'tags',) in tables)
		self.assertTrue((u'files',) in tables)
		self.assertTrue((u'other_files',) in tables)
		# close DB connection
		db.close()

	def test_add(self):
		'test that the addition of files, items and extra files works properly'
		# initialize the DB connection to check the addition
		db = sqlite3.connect(self._db_path)
		cur = db.cursor()
		# test the addition of a poor file
		test_file = self.test_file_poor_1
		self._db.add_poor_file(test_file['path'], test_file['file_checksum'], test_file['content_checksum'])
		for tag_name in test_file.keys():
			cur.execute("SELECT " + tag_name + " FROM files;")
			tag = cur.fetchone()
			self.assertTrue(tag == (test_file[tag_name],))
		# test the addition of a rich file
		test_file = self.test_file_rich_1
		self._db.add_rich_file(test_file['path'], test_file['file_checksum'], test_file['content_checksum'],
			test_file['tags'])
		for tag_name in test_file.keys():
			if tag_name != 'tags':
				cur.execute("SELECT " + tag_name + " FROM files WHERE path = '" + test_file['path'] + "';")
				tag = cur.fetchone()
				self.assertTrue(tag == (test_file[tag_name],))
			else:
				for tag_name in self.test_tags.keys():
					cur.execute("SELECT " + tag_name + " FROM tags LEFT JOIN files WHERE path = '" + \
						test_file['path'] + "';")
					tag = cur.fetchone()
					self.assertTrue(tag == (self.test_tags[tag_name],))
		# test the addition of an item
		test_item = self.test_item
		self._db.add_item(test_item['name'])
		cur.execute("SELECT name FROM simple_items;")
		values = cur.fetchall()
		self.assertTrue(len(values) == 1)
		self.assertTrue(values[0][0] == test_item['name'])
		# test the addition of a content file to the item
		self._db.add_item_content(test_item['name'], test_item['content_file'])
		cur.execute("SELECT content_file FROM simple_items;")
		value = cur.fetchone()[0]
		cur.execute("SELECT path FROM files WHERE file_id = %d;" % value)
		value = cur.fetchone()[0]
		self.assertTrue(value == self.test_item['content_file'])
		# test the addition of a tags file to the item
		self._db.add_item_tags(test_item['name'], test_item['tags_file'])
		cur.execute("SELECT tags_file FROM simple_items;")
		value = cur.fetchone()[0]
		cur.execute("SELECT path FROM files WHERE file_id = %d;" % value)
		value = cur.fetchone()[0]
		self.assertTrue(value == self.test_item['tags_file'])
		# test the addition of an extra file to the item
		test_file = self.test_file_poor_2
		self._db.add_poor_file(test_file['path'], test_file['file_checksum'], test_file['content_checksum'])
		self._db.add_extra_file(test_file['path'], test_item['name'])
		# close DB connection
		db.close()

	def test_get_item(self):
		'test that getting an item works'
		# add the item
		test_file = self.test_file_poor_1
		self._db.add_poor_file(test_file['path'], test_file['file_checksum'], test_file['content_checksum'])
		test_file = self.test_file_rich_1
		self._db.add_rich_file(test_file['path'], test_file['file_checksum'], test_file['content_checksum'],
			test_file['tags'])
		test_item = self.test_item
		self._db.add_item(test_item['name'])
		self._db.add_item_content(test_item['name'], test_item['content_file'])
		self._db.add_item_tags(test_item['name'], test_item['tags_file'])
		test_file = self.test_file_poor_2
		self._db.add_poor_file(test_file['path'], test_file['file_checksum'], test_file['content_checksum'])
		self._db.add_extra_file(test_file['path'], test_item['name'])
		test_file = self.test_file_poor_3
		self._db.add_poor_file(test_file['path'], test_file['file_checksum'], test_file['content_checksum'])
		self._db.add_extra_file(test_file['path'], test_item['name'])
		# test the item get
		(item_name, content_file, tags_file, extra_files) = self._db.get_item(self.test_item['name'])
		self.assertTrue(content_file == self.test_item['content_file'])
		self.assertTrue(tags_file == self.test_item['tags_file'])
		self.assertTrue(extra_files == [self.test_file_poor_2['path'], self.test_file_poor_3['path']])

class TestFilesHandler(unittest.TestCase):
	_jpeg_file_path = "./test.jpg"

	def setUp(self):
		# instanciate a FileHandler object
		self._fileshandler = files_handler.FilesHandler()	

	def teardown(self):
		# eliminate FileHandler object to close connection to DB and delete it
		db_path = self._fileshandler._db._db_path
		self._filehandler = None
		os.remove(db_path)

	def test_add_jpeg(self):
		self._fileshandler.add_file(self._jpeg_file_path)
		# initialize the DB connection to check the addition
		db = sqlite3.connect(self._fileshandler._db._db_path)
		cur = db.cursor()
		# expected tag values are:
		# FileName = "test.jpg"
		# Model = "Canon DIGITAL IXUS 50"
		# Software = "f-spot version 0.4.0"
		# DateTimeOriginal = "2006:01:01 04:06:15"
		# CreateDate = "2006:01:01 05:06:15"
		# ImageWidth = "2592"
		# ImageHeight = "1944"

	def test_add_video(self):
		pass

	def test_add_audio(self):
		pass

#class TestItemsHandler(unittest.TestCase):
#	def __init__(self):
#		pass
#
#	def setUp(self):
#		pass
#
#	def tearDown(self):
#		pass
#
#	def testGetItem(self):
#		pass
#
if __name__ == '__main__':
	unittest.main()
