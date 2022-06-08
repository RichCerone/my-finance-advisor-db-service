import unittest

from db_service.DbService import DbService, DbOptions, CosmosResourceNotFoundError
from unittest.mock import Mock

class DeleteTests(unittest.TestCase):
    
    def setUp(self) -> None:
       self.dbOptions = DbOptions("test_endpoint", "test_key", "test_db_id", "test_container_id")

    def tearDown(self) -> None:
        self.dbOptions = None

    # Asserts an item can be deleted.
    def test_delete_deletes_item(self):
        mock_container = Mock()
        mock_container.delete_item.return_value = None

        dbService = DbService(self.dbOptions)
        dbService.container = mock_container

        with self.assertLogs(level="INFO"):
            dbService.delete("user::test", "test")

        dbService.container.delete_item.assert_called_once()

    # Asserts a ValueError is raised if bad parameters are passed.
    def test_delete_raises_value_error(self):
        dbService = DbService(self.dbOptions)

        with self.assertLogs(level="ERROR"):
            with self.assertRaises(ValueError):
                dbService.delete(" ", "test")

        with self.assertLogs(level="ERROR"):
            with self.assertRaises(ValueError):
                dbService.delete("user::test", " ")

    # Asserts a CosmosResourceNotFoundError is raised if the item cannot be found to be deleted.
    def test_delete_raises_cosmos_resource_not_found_error(self):
        mock_container = Mock()
        mock_container.delete_item.side_effect = CosmosResourceNotFoundError()

        dbService = DbService(self.dbOptions)
        dbService.container = mock_container
        
        with self.assertLogs(level="ERROR"):
            with self.assertRaises(CosmosResourceNotFoundError):
                dbService.delete("user::test", "test")
        
        dbService.container.delete_item.assert_called_once()

    # Asserts an Exception is raised if an unexpected error occurs.
    def test_delete_raises_exception(self):
        mock_container = Mock()
        mock_container.delete_item.side_effect = Exception()

        dbService = DbService(self.dbOptions)
        dbService.container = mock_container
        
        with self.assertLogs(level="ERROR"):
            with self.assertRaises(Exception):
                dbService.delete("user::test", "test")
        
        dbService.container.delete_item.assert_called_once()

