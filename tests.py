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
		self.test_jpeg_file = {'path': u'/this/is/a/path',
							'file_checksum': u'f1l3ch3cksum',
							'content_checksum': u'c0nt3ntch3cksum',
							'tags': self.test_tags
							}
		self.test_other_file = {'path': u'/this/is/a/path',
							'file_checksum': u'f1l3ch3cksum',
							'content_checksum': u'c0nt3ntch3cksum'
							}
		self.test_item = {'name': u'/this/is/a/name', 
							'content_file': u'/this/is/a/path/1',
							'tags_file': u'/this/is/a/path/2'
							}
		self.test_extra_file = {'file_path': u'/this/is/a/path',
							'item_name': u'/this/is/a/name'
							}

	def _add_sample_item(self):
		test_item = self.test_item
		test_file_1 = self.test_other_file
		test_file_2 = self.test_jpeg_file
		self._db.add_other_file(test_item['content_file'], test_file_1['file_checksum'], 
					test_file_1['content_checksum'])
		self._db.add_jpeg_file(test_item['tags_file'], test_file_2['file_checksum'],
					test_file_2['content_checksum'], test_file_2['tags'])
		self._db.add_item(test_item['name'], test_item['content_file'], test_item['tags_file'])

	def test_01_db_creation(self):
		'test that the DB exists and has the expected tables'
		self.assertTrue(os.path.exists(self._db_path))
		db = sqlite3.connect(self._db_path)
		cur = db.cursor()
		cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
		tables = cur.fetchall()
		self.assertTrue((u'items',) in tables)
		self.assertTrue((u'tags',) in tables)
		self.assertTrue((u'files',) in tables)
		db.close()
		os.remove(self._db_path)

	def test_02_add_tags(self):
		'test that the addition of tags works properly'
		test_tags = self.test_tags
		self._db.add_tags(test_tags['model'], test_tags['software'], test_tags['date_time_original'],
			test_tags['create_date'], test_tags['image_width'],	test_tags['image_height'],
			test_tags['tags_list'],	test_tags['hierarchical_subject'], test_tags['subject'], test_tags['keywords'])
		db = sqlite3.connect(self._db_path)
		cur = db.cursor()
		for tag_name in test_tags.keys():
			cur.execute("SELECT " + tag_name + " FROM tags;")
			tag = cur.fetchone()
			self.assertTrue(tag == (test_tags[tag_name],))
		db.close()
		os.remove(self._db_path)

	def test_03_add_jpeg_file(self):
		'test that the addition of JPEG files works properly'
		test_file = self.test_jpeg_file
		self._db.add_jpeg_file(test_file['path'], test_file['file_checksum'], test_file['content_checksum'],
			test_file['tags'])
		db = sqlite3.connect(self._db_path)
		cur = db.cursor()
		for tag_name in test_file.keys():
			if tag_name != 'tags':
				cur.execute("SELECT " + tag_name + " FROM files;")
				tag = cur.fetchone()
				self.assertTrue(tag == (test_file[tag_name],))
			else:
				test_tags = self.test_tags
				for tag_name in test_tags.keys():
					cur.execute("SELECT " + tag_name + " FROM tags LEFT JOIN files;")
					tag = cur.fetchone()
					self.assertTrue(tag == (test_tags[tag_name],))
		db.close()
		os.remove(self._db_path)

	def test_04_add_other_file(self):
		'test that the addition of non-JPEG files works properly'
		test_file = self.test_other_file
		self._db.add_other_file(test_file['path'], test_file['file_checksum'], test_file['content_checksum'])
		db = sqlite3.connect(self._db_path)
		cur = db.cursor()
		for tag_name in test_file.keys():
			cur.execute("SELECT " + tag_name + " FROM files;")
			tag = cur.fetchone()
			self.assertTrue(tag == (test_file[tag_name],))
		db.close()
		os.remove(self._db_path)

	def test_05_add_item(self):
		'test that the addition of items works properly'
		self._add_sample_item()
		db = sqlite3.connect(self._db_path)
		cur = db.cursor()
		test_item = self.test_item
		for tag_name in test_item.keys():
			cur.execute("SELECT " + tag_name + " FROM items;")
			tag = cur.fetchone()
			self.assertTrue(tag == (test_item[tag_name],))
		db.close()
		os.remove(self._db_path)

	def test_06_add_extra_file(self):
		'test that the addition of relationships with extra files works properly'
		test_file_3 = self.test_other_file
		test_extra_file = self.test_extra_file
		self._add_sample_item()
		self._db.add_other_file(test_extra_file['file_path'], test_file_3['file_checksum'], 
					test_file_3['content_checksum'])
		self._db.add_extra_file(test_extra_file['file_path'], test_extra_file['item_name'])
		db = sqlite3.connect(self._db_path)
		cur = db.cursor()
		for tag_name in test_file.keys():
			cur.execute("SELECT " + tag_name + " FROM files;")
			tag = cur.fetchone()
			self.assertTrue(tag == (test_file[tag_name],))
		db.close()
		os.remove(self._db_path)

if __name__ == '__main__':
	unittest.main()
