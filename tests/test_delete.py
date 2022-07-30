import unittest

from src.db_service.DbService import DbService, DbOptions, CosmosResourceNotFoundError
from unittest.mock import Mock

class DeleteTests(unittest.TestCase):
    
    def setUp(self) -> None:
       self.db_options = DbOptions("test_endpoint", "test_key", "test_db_id", "test_container_id")

    def tearDown(self) -> None:
        self.db_options = None

    # Asserts an item can be deleted.
    def test_delete_deletes_item(self):
        mock_container = Mock()
        mock_container.delete_item.return_value = None

        db_service = DbService(self.db_options)
        db_service.container = mock_container

        with self.assertLogs(level="INFO"):
            db_service.delete("user::test", "test")

        db_service.container.delete_item.assert_called_once()

    # Asserts a ValueError is raised if bad parameters are passed.
    def test_delete_raises_value_error(self):
        db_service = DbService(self.db_options)

        with self.assertLogs(level="ERROR"):
            with self.assertRaises(ValueError):
                db_service.delete(" ", "test")

        with self.assertLogs(level="ERROR"):
            with self.assertRaises(ValueError):
                db_service.delete("user::test", " ")

    # Asserts a CosmosResourceNotFoundError is raised if the item cannot be found to be deleted.
    def test_delete_raises_cosmos_resource_not_found_error(self):
        mock_container = Mock()
        mock_container.delete_item.side_effect = CosmosResourceNotFoundError()

        db_service = DbService(self.db_options)
        db_service.container = mock_container
        
        with self.assertLogs(level="ERROR"):
            with self.assertRaises(CosmosResourceNotFoundError):
                db_service.delete("user::test", "test")
        
        db_service.container.delete_item.assert_called_once()

    # Asserts an Exception is raised if an unexpected error occurs.
    def test_delete_raises_exception(self):
        mock_container = Mock()
        mock_container.delete_item.side_effect = Exception()

        db_service = DbService(self.db_options)
        db_service.container = mock_container
        
        with self.assertLogs(level="ERROR"):
            with self.assertRaises(Exception):
                db_service.delete("user::test", "test")
        
        db_service.container.delete_item.assert_called_once()

