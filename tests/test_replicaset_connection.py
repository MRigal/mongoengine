import unittest
from nose.plugins.skip import SkipTest

from pymongo import ReadPreference, MongoClient


import mongoengine
from mongoengine.connection import ConnectionError, connect
from mongoengine.python_support import IS_PYMONGO_3

if IS_PYMONGO_3:
    from pymongo.errors import ServerSelectionTimeoutError as sste
else:
    from mongoengine.connection import ConnectionError as sste

CONN_CLASS = MongoClient
READ_PREF = ReadPreference.SECONDARY


class ConnectionTest(unittest.TestCase):

    def setUp(self):
        mongoengine.connection._connection_settings = {}
        mongoengine.connection._connections = {}
        mongoengine.connection._dbs = {}

    def tearDown(self):
        mongoengine.connection._connection_settings = {}
        mongoengine.connection._connections = {}
        mongoengine.connection._dbs = {}

    def test_replicaset_uri_passes_read_preference(self):
        """Requires a replica set called "rs" on port 27017
        """

        try:
            conn = connect(db='mongoenginetest',
                           host="mongodb://localhost/mongoenginetest?replicaSet=rs",
                           read_preference=READ_PREF,
                           serverSelectionTimeoutMS=500)
            conn.admin.command("ismaster")
        except (ConnectionError, sste), e:
            raise SkipTest('ReplicaSet test only works if you have a replica set rs running on port 27017')

        self.assertEqual(conn.read_preference, READ_PREF)

if __name__ == '__main__':
    unittest.main()
