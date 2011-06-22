#!/usr/bin/python

import unittest
import sqlite3
import os

import db_backend

class TestDbBackend(unittest.TestCase):
	def setUp(self):
		self._db = db_backend.DbConnector()	
		self._db_path = self._db._db_path
		self.test_tags = {'model': u'mod',
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
		self.test_file_poor_1 = {'path': u'/this/is/a/path_1',
							'file_checksum': u'f1l3ch3cksum1',
							'content_checksum': u'c0nt3ntch3cksum1'
							}
		self.test_file_rich_1 = {'path': u'/this/is/a/path_2',
							'file_checksum': u'f1l3ch3cksum2',
							'content_checksum': u'c0nt3ntch3cksum2',
							'tags': self.test_tags
							}
		self.test_file_poor_2 = {'path': u'/this/is/a/path_3',
							'file_checksum': u'f1l3ch3cksum3',
							'content_checksum': u'c0nt3ntch3cksum3'
							}
		self.test_item = {'name': u'/this/is/a/name', 
							'content_file': self.test_file_poor_1['path'],
							'tags_file': self.test_file_rich_1['path']
							}
		self.test_extra_file = {'file': self.test_file_poor_2['path'],
							'item': self.test_item['name']
							}

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
		self.assertTrue((u'items',) in tables)
		self.assertTrue((u'tags',) in tables)
		self.assertTrue((u'files',) in tables)
		self.assertTrue((u'other_files',) in tables)
		# close DB connection and delete it
		db.close()
		os.remove(self._db_path)

	def _add_poor_file(self):
		test_file = self.test_file_poor_1
		self._db.add_poor_file(test_file['path'], test_file['file_checksum'], test_file['content_checksum'])

	def test_add_poor_file(self):
		'test that the addition of files without metadata works properly'
		# add a poor file
		self._add_poor_file()
		# initialize the DB connection to check the addition
		db = sqlite3.connect(self._db_path)
		cur = db.cursor()
		# check that the DB contains the file entry
		for tag_name in self.test_file_poor_1.keys():
			cur.execute("SELECT " + tag_name + " FROM files;")
			tag = cur.fetchone()
			self.assertTrue(tag == (self.test_file_poor_1[tag_name],))
		# close DB connection and delete it
		db.close()
		os.remove(self._db_path)

	def _add_tags(self):
		test_tags = self.test_tags
		self._db.add_tags(test_tags['model'], test_tags['software'], test_tags['date_time_original'],
			test_tags['create_date'], test_tags['image_width'],	test_tags['image_height'],
			test_tags['tags_list'],	test_tags['hierarchical_subject'], test_tags['subject'], test_tags['keywords'])

	def test_add_tags(self):
		'test that the addition of tags works properly'
		# add the tags of a JPEG file
		self._add_tags()
		# initialize the DB connection to check the addition
		db = sqlite3.connect(self._db_path)
		cur = db.cursor()
		# check that the DB contains the tags entry
		for tag_name in self.test_tags.keys():
			cur.execute("SELECT " + tag_name + " FROM tags;")
			tag = cur.fetchone()
			self.assertTrue(tag == (self.test_tags[tag_name],))
		# close DB connection and delete it
		db.close()
		os.remove(self._db_path)

	def _add_rich_file(self):
		test_file = self.test_file_rich_1
		self._db.add_rich_file(test_file['path'], test_file['file_checksum'], test_file['content_checksum'],
			test_file['tags'])

	def test_add_rich_file(self):
		'test that the addition of files with metadata works properly'
		# add a rich file
		self._add_rich_file()
		# initialize the DB connection to check the addition
		db = sqlite3.connect(self._db_path)
		cur = db.cursor()
		# check that the DB contains the file entry and the corresponding tags entry
		for tag_name in self.test_file_rich_1.keys():
			if tag_name != 'tags':
				cur.execute("SELECT " + tag_name + " FROM files;")
				tag = cur.fetchone()
				self.assertTrue(tag == (self.test_file_rich_1[tag_name],))
			else:
				for tag_name in self.test_tags.keys():
					cur.execute("SELECT " + tag_name + " FROM tags LEFT JOIN files;")
					tag = cur.fetchone()
					self.assertTrue(tag == (self.test_tags[tag_name],))
		# close DB connection and delete it
		db.close()
		os.remove(self._db_path)

	def _add_item(self):
		test_item = self.test_item
		self._db.add_item(test_item['name'])

	def test_add_item(self):
		'test that the addition of items works properly'
		# add the item
		self._add_item()
		# initialize the DB connection to check the addition
		db = sqlite3.connect(self._db_path)
		cur = db.cursor()
		# check that the DB contains the expected item
		cur.execute("SELECT name FROM items;")
		values = cur.fetchall()
		self.assertTrue(len(values) == 1)
		self.assertTrue(values[0][0] == self.test_item['name'])
		# close DB connection and delete it
		db.close()
		os.remove(self._db_path)

	def _add_item_content(self):
		test_item = self.test_item
		self._add_poor_file()
		self._db.add_item(test_item['name'])
		self._db.add_item_content(test_item['name'], test_item['content_file'])

	def test_add_item_content(self):
		'test that the addition of a content file to an item works properly'
		# add the item
		self._add_item_content()
		# initialize the DB connection to check the addition
		db = sqlite3.connect(self._db_path)
		cur = db.cursor()
		# check that the DB contains the expected item
		cur.execute("SELECT name FROM items;")
		values = cur.fetchall()
		self.assertTrue(len(values) == 1)
		self.assertTrue(values[0][0] == self.test_item['name'])
		# check that content_file has been properly assigned
		cur.execute("SELECT content_file FROM items;")
		value = cur.fetchone()[0]
		cur.execute("SELECT path FROM files WHERE file_id = %d;" % value)
		value = cur.fetchone()[0]
		self.assertTrue(value == self.test_item['content_file'])
		# close DB connection and delete it
		db.close()
		os.remove(self._db_path)

	def _add_item_tags(self):
		test_item = self.test_item
		self._add_rich_file()
		self._db.add_item(test_item['name'])
		self._db.add_item_tags(test_item['name'], test_item['tags_file'])

	def test_add_item_tags(self):
		'test that the addition of a tags file to an item works properly'
		# add the item
		self._add_item_tags()
		# initialize the DB connection to check the addition
		db = sqlite3.connect(self._db_path)
		cur = db.cursor()
		# check that the DB contains the expected item
		cur.execute("SELECT name FROM items;")
		values = cur.fetchall()
		self.assertTrue(len(values) == 1)
		self.assertTrue(values[0][0] == self.test_item['name'])
		# check that tags_file has been properly assigned
		cur.execute("SELECT tags_file FROM items;")
		value = cur.fetchone()[0]
		cur.execute("SELECT path FROM files WHERE file_id = %d;" % value)
		value = cur.fetchone()[0]
		self.assertTrue(value == self.test_item['tags_file'])
		# close DB connection and delete it
		db.close()
		os.remove(self._db_path)

	def _add_full_item(self):
		test_item = self.test_item
		self._add_rich_file()
		self._add_poor_file()
		self._db.add_item(test_item['name'])
		self._db.add_item_content(test_item['name'], test_item['content_file'])
		self._db.add_item_tags(test_item['name'], test_item['tags_file'])

	def test_get_item(self):
		'test that getting an item works'
		return
		#add the item
		self._add_full_item()
		#get the item
		item = self._db.get_item(test_item['name'])

	def _add_extra_file(self):
		self._add_item()
		test_file = self.test_file_poor_2
		test_item = self.test_item
		self._db.add_poor_file(test_file['path'], test_file['file_checksum'], test_file['content_checksum'])
		self._db.add_extra_file(test_file['path'], test_item['name'])

	def test_add_extra_file(self):
		'test that the addition of relationships with extra files works properly'
		# add the item
		self._add_extra_file()
		# initialize the DB connection to check the addition
		db = sqlite3.connect(self._db_path)
		cur = db.cursor()
		# check that the DB contains a single extra file association, as expected
		cur.execute("SELECT * FROM other_files;")
		values = cur.fetchall()
		self.assertTrue(len(values) == 1)
		# check that file has been properly assigned
		cur.execute("SELECT file FROM other_files;")
		value = cur.fetchone()[0]
		cur.execute("SELECT path FROM files WHERE file_id = %d;" % value)
		value = cur.fetchone()[0]
		self.assertTrue(value == self.test_extra_file['file'])
		# check that item has been properly assigned
		cur.execute("SELECT item FROM other_files;")
		value = cur.fetchone()[0]
		cur.execute("SELECT name FROM items WHERE item_id = %d;" % value)
		value = cur.fetchone()[0]
		self.assertTrue(value == self.test_extra_file['item'])
		# close DB connection and delete it
		db.close()
		os.remove(self._db_path)

if __name__ == '__main__':
	unittest.main()
