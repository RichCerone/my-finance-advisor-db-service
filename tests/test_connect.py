import unittest

from src.db_service.DbService import DbService, DbOptions
from unittest.mock import Mock, patch

class ConnectTests(unittest.TestCase):
    def setUp(self) -> None:
       self.db_options = DbOptions("test_endpoint", "test_key", "test_db_id", "test_container_id")

    def tearDown(self) -> None:
        self.db_options = None

    # Assert that we can connect to the database.
    @patch("src.db_service.DbService.CosmosClient")
    def test_connect_connects(self, mock_cosmos_client):
        db_service = DbService(self.db_options)
        
        db_service.client = mock_cosmos_client
        db_service.client.get_database_client.return_value = True

        db_service.db = Mock()
        db_service.db.get_container_client.return_value = True

        with self.assertLogs(level="INFO"):
            db_service.connect()

        db_service.client.get_database_client.assert_called_once()
        db_service.db.get_container_client.assert_called_once()

        self.assertIsNotNone(db_service.container)
        self.assertTrue(db_service.container, True)

    # Assert that an exception is raised if the dbOptions are invalid.
    def test_connect_invalid_db_options(self):
        self.db_options = None
        db_service = DbService(self.db_options)

        with self.assertLogs(level="ERROR"):
            with self.assertRaises(Exception):
                db_service.connect()                
    
    # Assert that an exception is thrown if the connection cannot be opened.
    @patch("src.db_service.DbService.CosmosClient")    
    def test_connect_cannot_open_connection(self, mock_cosmos_client):
        mock_cosmos_client.side_effect = Exception()

        db_service = DbService(self.db_options)
        with self.assertLogs(level="ERROR"):
            with self.assertRaises(Exception):
                db_service.connect()                
        
        mock_cosmos_client.assert_called_once()
    
    # Assert we throw an exception if we cannot get the db.
    @patch("src.db_service.DbService.CosmosClient")    
    def test_connect_cannot_get_db(self, mock_cosmos_client):
        mock_cosmos_client.return_value = mock_cosmos_client

        db_service = DbService(self.db_options)
        
        db_service.client = mock_cosmos_client
        db_service.client.get_database_client.side_effect = Exception()

        with self.assertLogs(level="ERROR"):
            with self.assertRaises(Exception):
                db_service.connect()                
        
        db_service.client.assert_called_once()
        db_service.client.get_database_client.assert_called_once()

    # Assert we throw an exception if we cannot get the container.
    @patch("src.db_service.DbService.CosmosClient")
    def test_connect_cannot_get_container(self, mock_cosmos_client):
        mock_cosmos_client.return_value = mock_cosmos_client

        db_service = DbService(self.db_options)

        db_service.client = mock_cosmos_client
        db_service.db = Mock()
        db_service.client.get_database_client.return_value =db_service.db
        db_service.db.get_container_client.side_effect = Exception()

        with self.assertLogs(level="ERROR"):
            with self.assertRaises(Exception):
                db_service.connect()

        db_service.client.assert_called_once()
        db_service.client.get_database_client.assert_called_once()
        db_service.db.get_container_client.assert_called_once()


