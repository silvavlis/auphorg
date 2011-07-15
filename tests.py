#!/usr/bin/python
# -*- coding: utf-8 -*-

import unittest
import sqlite3
import os
import shutil

import db_backend
import files_handler

class TestDbBackend(unittest.TestCase):
	test_tags = {'Model': u'mod',
				'Software': u'soft',
				'DateTimeOriginal': u'dat_org',
				'CreateDate': u'creat_dat',
				'ImageWidth': u'img_w',
				'ImageHeight': u'img_h',
				'TagsList': u'tags',
				'HierarchicalSubject': u'hierc_subj',
				'Subject': u'subjs',
				'Keywords': u'keyws'
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
		# test the addition of a poor image
		test_file = self.test_file_poor_1
		self._db.add_non_raw_file(test_file['path'], test_file['file_checksum'], test_file['content_checksum'])
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
		self._db.add_non_raw_file(test_file['path'], test_file['file_checksum'], test_file['content_checksum'])
		self._db.add_extra_file(test_file['path'], test_item['name'])
		# close DB connection
		db.close()

	def test_get(self):
		'test that getting an item works'
		# add the item
		test_file = self.test_file_poor_1
		self._db.add_non_raw_file(test_file['path'], test_file['file_checksum'], test_file['content_checksum'])
		test_file = self.test_file_rich_1
		self._db.add_rich_file(test_file['path'], test_file['file_checksum'], test_file['content_checksum'],
			test_file['tags'])
		test_item = self.test_item
		self._db.add_item(test_item['name'])
		self._db.add_item_content(test_item['name'], test_item['content_file'])
		self._db.add_item_tags(test_item['name'], test_item['tags_file'])
		test_file = self.test_file_poor_2
		self._db.add_non_raw_file(test_file['path'], test_file['file_checksum'], test_file['content_checksum'])
		self._db.add_extra_file(test_file['path'], test_item['name'])
		test_file = self.test_file_poor_3
		self._db.add_non_raw_file(test_file['path'], test_file['file_checksum'], test_file['content_checksum'])
		self._db.add_extra_file(test_file['path'], test_item['name'])
		# test the file tags get
		test_file = self.test_file_rich_1
		tags = self._db.get_rich_file_tags(test_file['path'])
		for tag in self.test_tags:
			self.assertTrue(tags[tag] == self.test_tags[tag])	
		# test the item get
		(item_name, content_file, tags_file, extra_files) = self._db.get_item(self.test_item['name'])
		self.assertTrue(content_file == self.test_item['content_file'])
		self.assertTrue(tags_file == self.test_item['tags_file'])
		self.assertTrue(extra_files == [self.test_file_poor_2['path'], self.test_file_poor_3['path']])

class TestFilesHandler(unittest.TestCase):
	_jpeg_file_path = "./test.jpg"
	_tif_file_path = "./test.tif"
	_raw_file_path = "./test.raw"
	_video_file_path = "./test.avi"
	_audio_file_path = "./test.wav"

	def setUp(self):
		# instanciate a FileHandler object
		self._fileshandler = files_handler.FilesHandler()	

	def tearDown(self):
		# eliminate FileHandler object to close connection to DB and delete it
		db_path = self._fileshandler._db._db_path
		self._filehandler = None
		os.remove(db_path)

	def test_add_jpeg(self):
		'tests that the addition of a JPEG file works'
		path = self._jpeg_file_path
		# add the file
		self._fileshandler.add_file(path)
		# initialize the DB connection to check the addition
		db = sqlite3.connect(self._fileshandler._db._db_path)
		cur = db.cursor()
		# test the file checksums
		cur.execute("SELECT file_checksum, content_checksum FROM files ' + \
					'WHERE path = ?;", [path])
		(file_checksum, content_checksum)= cur.fetchone()
		FILE_CHECKSUM = 'b6997bddbfbf70f903e664dfef363a63ce4e464c494480' + \
			'a79c5f601485e6e481389e7da900a980a5bea7aca0b6ab040853ffababcd8' + \
			'5aca8360dccd514a1d676'
		self.assertTrue(file_checksum == FILE_CHECKSUM)
		CONTENT_CHECKSUM = '562fbb377e28a0577356a35ad1b17dbdbdccbdfb74df810c2' + \
			'e156cecd27613e209675c404a1eac05c0e2dbabedc92abf77f3267f1d14c7d2c' + \
			'246cf1bd49f7724'
		self.assertTrue(content_checksum == CONTENT_CHECKSUM)
		# test the expected tag values
		cur.execute("SELECT tags.* FROM tags, files " + \
					"WHERE files.tags = tags.tags_id AND files.path = ?;", \
					[path])
		tags = cur.fetchone()
		# Model = Canon DIGITAL IXUS 50
		self.assertTrue(tags[1] == u'Canon DIGITAL IXUS 50')
		# Software = f-spot version 0.4.0
		self.assertTrue(tags[2] == u'f-spot version 0.4.0')
		# DateTimeOriginal = 2006:01:01 04:06:15
		self.assertTrue(tags[3] == u'2006:01:01 04:06:15')
		# CreateDate = 2006:01:01 05:06:15
		self.assertTrue(tags[4] == u'2006:01:01 05:06:15')
		# ImageWidth = 2592
		self.assertTrue(tags[5] == u'2592')
		# ImageHeight = 1944
		self.assertTrue(tags[6] == u'1944')
		# TagsList = People/Comuna/Roser, Places/España/Zaragoza, 
		# 	People/Comuna/ElenaSt, Places/España/Tarragona, People/Comuna/Eva, 
		# 	People/Comuna/Sonia, Nochevieja2005, JaviA, Bea, Mari, Ordenadas
		self.assertTrue(tags[7] == unicode('People/Comuna/Roser, ' + \
			'Places/España/Zaragoza, People/Comuna/ElenaSt, ' + \
			'Places/España/Tarragona, People/Comuna/Eva, ' + \
			'People/Comuna/Sonia, Nochevieja2005, JaviA, Bea, Mari, Ordenadas', \
			'iso-8859-15'))
		# HierarchicalSubject = People|Comuna|Roser, Places|España|Zaragoza, 
		# 	People|Comuna|ElenaSt, Places|España|Tarragona, People|Comuna|Eva, 
		# 	People|Comuna|Sonia, Nochevieja2005, JaviA, Bea, Mari, Ordenadas
		self.assertTrue(tags[8] == unicode('People|Comuna|Roser, ' + \
			'Places|España|Zaragoza, People|Comuna|ElenaSt, ' + \
			'Places|España|Tarragona, People|Comuna|Eva, ' + \
			'People|Comuna|Sonia, Nochevieja2005, JaviA, Bea, Mari, Ordenadas', \
			'iso-8859-15'))
		# Subject = Roser, Zaragoza, ElenaSt, Tarragona, Eva, Sonia, 
		# 	Nochevieja2005, JaviA, Bea, Mari, Ordenadas
		self.assertTrue(tags[9] == unicode('Roser, Zaragoza, ElenaSt, ' + \
			'Tarragona, Eva, Sonia, Nochevieja2005, JaviA, Bea, Mari, ' + \
			'Ordenadas', 'iso-8859-15'))
		# Keywords = Roser, Zaragoza, ElenaSt, Tarragona, Eva, Sonia, 
		# 	Nochevieja2005, JaviA, Bea, Mari, Ordenadas
		self.assertTrue(tags[10] == unicode('Roser, Zaragoza, ElenaSt, ' + \
			'Tarragona, Eva, Sonia, Nochevieja2005, JaviA, Bea, Mari, ' + \
			'Ordenadas', 'iso-8859-15'))

	def test_add_non_raw(self):
		'tests that the addition of a non-raw file works'
		path = self._tif_file_path
		# add the file
		self._fileshandler.add_file(path)
		# initialize the DB connection to check the addition
		db = sqlite3.connect(self._fileshandler._db._db_path)
		cur = db.cursor()
		# test the file checksums
		cur.execute("SELECT file_checksum, content_checksum FROM files ' + \
					'WHERE path = ?;", [path])
		(file_checksum, content_checksum)= cur.fetchone()
		FILE_CHECKSUM = '0ec5c06bb958b6ed4756f75d731e7db0eb5fc9c7793128' + \
			'42b84cc43e75a18f5ab79767db017cdfe03e32d5bac2269261c3d53f19' + \
			'07012c641b8400dfaba10ebf'
		self.assertTrue(file_checksum == FILE_CHECKSUM)
		CONTENT_CHECKSUM = '2f7361b7d0ad9f04a1739eb57c557431f68e9f63f7f' + \
			'4ac7343b22a0212d824152c109277b6a5f23a5ca47d8a598c9ba5b3820' + \
			'a4783737846cfb8a11816375213'
		self.assertTrue(content_checksum == CONTENT_CHECKSUM)

	def test_add_raw_file(self):
		'tests that the addition of a raw picture file works'
		path = self._raw_file_path
		# add the file
		self._fileshandler.add_file(path)
		# initialize the DB connection to check the addition
		db = sqlite3.connect(self._fileshandler._db._db_path)
		cur = db.cursor()
		# test the file checksums
		cur.execute("SELECT file_checksum FROM files ' + \
					'WHERE path = ?;", [path])
		(file_checksum,)= cur.fetchone()
		FILE_CHECKSUM = 'bb5cff62ca1f7afd7d259aa795225b011560ee8fb3fa18d122d2' + \
			'ccbea7fe9f37ee500c00941b96911a41eb341a9c14d168c23dcde76911204ed8' + \
			'1f1c2f215780'
		self.assertTrue(file_checksum == FILE_CHECKSUM)

	def test_add_video(self):
		'tests that the addition of a video file works'
		path = self._video_file_path
		# add the file
		self._fileshandler.add_file(path)
		# initialize the DB connection to check the addition
		db = sqlite3.connect(self._fileshandler._db._db_path)
		cur = db.cursor()
		# test the file checksums
		cur.execute("SELECT file_checksum, content_checksum FROM files ' + \
					'WHERE path = ?;", [path])
		(file_checksum, content_checksum)= cur.fetchone()
		FILE_CHECKSUM = 'f5df0c2e9700f1a959632864681ee0f22e4e4f3b0de99a29f259' + \
			'52bc2e128bb17b9b98f040c02320a3770701d762d0b179bfe2d1089cff4a9030' + \
			'84be08c18297'
		self.assertTrue(file_checksum == FILE_CHECKSUM)
		CONTENT_CHECKSUM = 'a052484faa9a42f7e7c4aa5af6d4a76fd79a6dc167d322926' + \
			'06ed10a50a0adc35aa2c73ffad97389b387d9bca7da3239b14817ffd5b14789a' + \
			'cd9533bc1981bd4'
		self.assertTrue(content_checksum == CONTENT_CHECKSUM)

	def test_add_audio(self):
		'tests that the addition of an audio file works'
		path = self._audio_file_path
		# add the file
		self._fileshandler.add_file(path)
		# initialize the DB connection to check the addition
		db = sqlite3.connect(self._fileshandler._db._db_path)
		cur = db.cursor()
		# test the file checksums
		cur.execute("SELECT file_checksum, content_checksum FROM files ' + \
					'WHERE path = ?;", [path])
		(file_checksum, content_checksum)= cur.fetchone()
		FILE_CHECKSUM = '720b5044c891d29212b4e8b0289d2b5dcec33a040208be7e3cbe' + \
			'1c331a2a403a1041d94d13f68d9b0961f4721619a250e50628bf9ae26220ff72' + \
			'05f10e5a871e'
		self.assertTrue(file_checksum == FILE_CHECKSUM)
		CONTENT_CHECKSUM = '9a0a83575d79c38052a07d50b809753fab5ca31680de2bfa2' + \
			'67ead08548f15f82e4e7b2695874c1b7dc363427c7379636f7d6b3864a65d382' + \
			'79a5c45b8250758'
		self.assertTrue(content_checksum == CONTENT_CHECKSUM)

class TestTreeScanner(unittest.TestCase):
	def setUp(self):
		if os.path.exists('./testTree'):
			shutil.rmtree('./testTree')
		os.mkdir('./testTree')
		os.mkdir('./testTree/subdir1')
		shutil.copy('./test.jpg', './testTree/subdir1/test_1.jpg')
		shutil.copy('./test.jpg', './testTree/subdir1/test_2.jpg')
		shutil.copy('./test.avi', './testTree/subdir1/test_2.avi')
		shutil.copy('./test.jpg', './testTree/subdir1/test_3.jpg')
		shutil.copy('./test.wav', './testTree/subdir1/test_3.wav')
		os.mkdir('./testTree/subdir2')
		shutil.copy('./test.jpg', './testTree/subdir2/test_1.jpg')

	def tearDown(self):
		shutil.rmtree('./testTree')

	def test1(self):
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
