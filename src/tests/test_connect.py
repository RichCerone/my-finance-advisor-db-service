import unittest

from db_service.DbService import DbService, DbOptions
from unittest.mock import Mock, patch

class ConnectTests(unittest.TestCase):
    def setUp(self) -> None:
       self.dbOptions = DbOptions("test_endpoint", "test_key", "test_db_id", "test_container_id")

    def tearDown(self) -> None:
        self.dbOptions = None

    # Assert that we can connect to the database.
    @patch("db_service.DbService.CosmosClient")
    def test_connect_connects(self, mock_cosmos_client):
        dbService = DbService(self.dbOptions)
        
        dbService.client = mock_cosmos_client
        dbService.client.get_database_client.return_value = True

        dbService.db = Mock()
        dbService.db.get_container_client.return_value = True

        with self.assertLogs(level="INFO"):
            dbService.connect()

        dbService.client.get_database_client.assert_called_once()
        dbService.db.get_container_client.assert_called_once()

        self.assertIsNotNone(dbService.container)
        self.assertTrue(dbService.container, True)

    # Assert that an exception is raised if the dbOptions are invalid.
    def test_connect_invalid_db_options(self):
        self.dbOptions = None
        dbService = DbService(self.dbOptions)

        with self.assertLogs(level="ERROR"):
            with self.assertRaises(Exception):
                dbService.connect()                
    
    # Assert that an exception is thrown if the connection cannot be opened.
    @patch("db_service.DbService.CosmosClient")    
    def test_connect_cannot_open_connection(self, mock_cosmos_client):
        mock_cosmos_client.side_effect = Exception()

        dbService = DbService(self.dbOptions)
        with self.assertLogs(level="ERROR"):
            with self.assertRaises(Exception):
                dbService.connect()                
        
        mock_cosmos_client.assert_called_once()
    
    # Assert we throw an exception if we cannot get the db.
    @patch("db_service.DbService.CosmosClient")    
    def test_connect_cannot_get_db(self, mock_cosmos_client):
        mock_cosmos_client.return_value = mock_cosmos_client

        dbService = DbService(self.dbOptions)
        
        dbService.client = mock_cosmos_client
        dbService.client.get_database_client.side_effect = Exception()

        with self.assertLogs(level="ERROR"):
            with self.assertRaises(Exception):
                dbService.connect()                
        
        dbService.client.assert_called_once()
        dbService.client.get_database_client.assert_called_once()

    # Assert we throw an exception if we cannot get the container.
    @patch("db_service.DbService.CosmosClient")
    def test_connect_cannot_get_container(self, mock_cosmos_client):
        mock_cosmos_client.return_value = mock_cosmos_client

        dbService = DbService(self.dbOptions)

        dbService.client = mock_cosmos_client
        dbService.db = Mock()
        dbService.client.get_database_client.return_value =dbService.db
        dbService.db.get_container_client.side_effect = Exception()

        with self.assertLogs(level="ERROR"):
            with self.assertRaises(Exception):
                dbService.connect()

        dbService.client.assert_called_once()
        dbService.client.get_database_client.assert_called_once()
        dbService.db.get_container_client.assert_called_once()


