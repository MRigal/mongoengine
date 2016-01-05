import datetime
import unittest
from nose.plugins.skip import SkipTest

import pymongo
from pymongo.errors import OperationFailure
from bson.tz_util import utc

from mongoengine import Document, DateTimeField
import mongoengine.connection
from mongoengine.connection import connect, get_db, get_connection, register_connection, ConnectionError


class ConnectionTest(unittest.TestCase):

    def tearDown(self):
        mongoengine.connection._connection_settings = {}
        mongoengine.connection._connections = {}
        mongoengine.connection._dbs = {}

    def test_connect(self):
        """Ensure that the connect() method works properly.
        """
        connect('mongoenginetest')

        conn = get_connection()
        self.assertTrue(isinstance(conn, pymongo.mongo_client.MongoClient))

        db = get_db()
        self.assertTrue(isinstance(db, pymongo.database.Database))
        self.assertEqual(db.name, 'mongoenginetest')

        connect('mongoenginetest2', alias='testdb')
        conn = get_connection('testdb')
        self.assertTrue(isinstance(conn, pymongo.mongo_client.MongoClient))
        conn.admin.command("ismaster")

    def test_connect_in_mocking(self):
        """Ensure that the connect() method works properly in mocking.
        """
        try:
            import mongomock
        except ImportError:
            raise SkipTest('you need mongomock installed to run this testcase')

        connect('mongoenginetest', host='mongomock://localhost')
        conn = get_connection()
        self.assertTrue(isinstance(conn, mongomock.MongoClient))

        connect('mongoenginetest2', host='mongomock://localhost', alias='testdb')
        conn = get_connection('testdb')
        self.assertTrue(isinstance(conn, mongomock.MongoClient))

    def test_disconnect(self):
        """Ensure that the disconnect() method works properly
        """
        conn1 = connect('mongoenginetest')
        mongoengine.connection.disconnect()
        conn2 = connect('mongoenginetest')
        self.assertTrue(conn1 is not conn2)

    def test_sharing_connections(self):
        """Ensure that connections are shared when the connection settings are exactly the same
        """
        connect('mongoenginetests', alias='testdb1')
        expected_connection = get_connection('testdb1')

        connect('mongoenginetests', alias='testdb2')
        actual_connection = get_connection('testdb2')

        # Handle PyMongo 3+ Async Connection
        expected_connection.admin.command("ismaster")
        actual_connection.admin.command("ismaster")

        self.assertEqual(expected_connection, actual_connection)

    def test_connect_uri(self):
        """Ensure that the connect() method works properly with uri's
        """
        c = connect(db='mongoenginetest', alias='first')
        c.admin.command("ismaster")
        c.admin.system.users.delete_many({})
        c.mongoenginetest.system.users.delete_many({})

        c.admin.add_user("admin", "password")
        c.admin.authenticate("admin", "password")
        c.mongoenginetest.add_user("username", "password")

        c1 = connect(db="testdb_uri_bad", host='mongodb://test:password@localhost', connect=False, alias='c1')
        with self.assertRaises(OperationFailure):
            c1.admin.command("ismaster")

        connect(db="testdb_uri", host='mongodb://username:password@localhost/mongoenginetest', alias='c2')

        conn = get_connection('c2')
        self.assertTrue(isinstance(conn, pymongo.mongo_client.MongoClient))
        conn.admin.command("ismaster")

        db = get_db('c2')
        self.assertTrue(isinstance(db, pymongo.database.Database))
        self.assertEqual(db.name, 'mongoenginetest')

        c.admin.system.users.delete_many({})
        c.mongoenginetest.system.users.delete_many({})

    def test_connect_uri_without_db(self):
        """Ensure connect() method works properly with uri's without database_name
        """
        c = connect(db='mongoenginetest', alias='admin')
        c.admin.command("ismaster")
        c.admin.system.users.delete_many({})
        c.mongoenginetest.system.users.delete_many({})

        c.admin.add_user("admin", "password")
        c.admin.authenticate("admin", "password")
        c.mongoenginetest.add_user("username", "password")

        c1 = connect("testdb_uri_bad", host='mongodb://test:password@localhost', alias='bad_uri', connect=False)
        with self.assertRaises(OperationFailure):
            c1.admin.command("ismaster")

        connect("mongoenginetest", host='mongodb://localhost/')
        conn = get_connection()
        self.assertTrue(isinstance(conn, pymongo.mongo_client.MongoClient))

        db = get_db()
        self.assertTrue(isinstance(db, pymongo.database.Database))
        self.assertEqual(db.name, 'mongoenginetest')

        c.admin.system.users.delete_many({})
        c.mongoenginetest.system.users.delete_many({})

    def test_connect_uri_with_authsource(self):
        """Ensure that the connect() method works well with the option `authSource` in URI.
        This feature was introduced in MongoDB 2.4 and removed in 2.6
        """
        # Create users
        c = connect('mongoenginetest')
        c.admin.command("ismaster")
        c.admin.system.users.delete_many({})
        c.admin.add_user('username2', 'password')

        # Authentication fails without "authSource"
        test_conn = connect('mongoenginetest', alias='test1',
                            host='mongodb://username2:password@localhost/mongoenginetest', connect=False)
        with self.assertRaises(OperationFailure):
            test_conn.admin.command("ismaster")

        # Authentication succeeds with "authSource"
        new_conn = connect('mongoenginetest', alias='test2',
                           host='mongodb://username2:password@localhost/mongoenginetest?authSource=admin')
        new_conn.admin.command("ismaster")
        db = get_db('test2')
        self.assertTrue(isinstance(db, pymongo.database.Database))
        self.assertEqual(db.name, 'mongoenginetest')

        # Clear all users
        c.admin.system.users.delete_many({})

    def test_register_connection(self):
        """Ensure that connections with different aliases may be registered.
        """
        register_connection('testdb', 'mongoenginetest2')
        self.assertRaises(ConnectionError, get_connection)
        conn = get_connection('testdb')
        self.assertTrue(isinstance(conn, pymongo.mongo_client.MongoClient))

        db = get_db('testdb')
        self.assertTrue(isinstance(db, pymongo.database.Database))
        self.assertEqual(db.name, 'mongoenginetest2')

    def test_register_connection_defaults(self):
        """Ensure that defaults are used when the host and port are None.
        """
        register_connection('testdb', 'mongoenginetest', host=None, port=None)

        conn = get_connection('testdb')
        self.assertTrue(isinstance(conn, pymongo.mongo_client.MongoClient))

    def test_connection_kwargs(self):
        """Ensure that connection kwargs get passed to pymongo.
        """
        connect('mongoenginetest', alias='t1', tz_aware=True)
        conn = get_connection('t1')
        self.assertTrue(conn.codec_options.tz_aware)

        connect('mongoenginetest2', alias='t2')
        conn = get_connection('t2')
        self.assertFalse(conn.codec_options.tz_aware)

    def test_datetime(self):
        connect('mongoenginetest', tz_aware=True)
        d = datetime.datetime(2010, 5, 5, tzinfo=utc)

        class DateDoc(Document):
            the_date = DateTimeField(required=True)

        DateDoc.drop_collection()
        DateDoc(the_date=d).save()

        date_doc = DateDoc.objects.first()
        self.assertEqual(d, date_doc.the_date)

    def test_multiple_connection_settings(self):
        connect('mongoenginetest', alias='t1', host="localhost")
        connect('mongoenginetest2', alias='t2', host="127.0.0.1")
        mongo_connections = mongoengine.connection._connections
        self.assertEqual(len(mongo_connections.items()), 2)
        self.assertTrue('t1' in mongo_connections.keys())
        self.assertTrue('t2' in mongo_connections.keys())
        mongo_connections['t1'].admin.command("ismaster")
        mongo_connections['t2'].admin.command("ismaster")
        self.assertEqual(mongo_connections['t1'].address[0], 'localhost')
        self.assertEqual(mongo_connections['t2'].address[0], '127.0.0.1')


if __name__ == '__main__':
    unittest.main()
