import json
import unittest

from src.db_service.DbService import DbService, DbOptions, CosmosHttpResponseError
from tests.mocks.User import User
from unittest.mock import Mock

class GetTests(unittest.TestCase):

    def setUp(self) -> None:
       self.db_options = DbOptions("test_endpoint", "test_key", "test_db_id", "test_container_id")

    def tearDown(self) -> None:
        self.db_options = None

    # Asserts an item can be retrieved.
    def test_get_gets_item(self):
        userMock = User("test", "testing")
        mock_container = Mock()
        mock_container.read_item.return_value = userMock.__dict__

        db_service = DbService(self.db_options)
        db_service.container = mock_container

        with self.assertLogs(level="INFO"):
            result = db_service.get("user::test", "user")

        j = json.loads(result)
        user = User(**j)

        self.assertIsInstance(user, User)
        self.assertEqual("test", user.user)
        self.assertEqual("testing", user.password)

    # Asserts a ValueError is raised if the parameters are invalid.
    def test_get_raises_value_error(self):
        db_service = DbService(self.db_options)

        with self.assertLogs(level="ERROR"):
            with self.assertRaises(ValueError):
                db_service.get(" ", "test_partition")

        with self.assertLogs(level="ERROR"):
            with self.assertRaises(ValueError):
                db_service.get(None, "test_partition")
        
        with self.assertLogs(level="ERROR"):
            with self.assertRaises(ValueError):
                db_service.get("test", " ")

        with self.assertLogs(level="ERROR"):
            with self.assertRaises(ValueError):
                db_service.get("test", None)

    # Asserts None is returned if no item is found.
    def test_get_cannot_find_item(self):
        mock_container = Mock()
        mock_container.read_item.side_effect = CosmosHttpResponseError()

        db_service = DbService(self.db_options)
        db_service.container = mock_container

        with self.assertLogs(level="WARNING"):
            result = db_service.get("test", "test_partition")

        self.assertEqual(None, result)
        db_service.container.read_item.assert_called_once()

    # Asserts an Exception is raised if an unexpected error occurs.
    def test_get_raises_exception(self):
        mock_container = Mock()
        mock_container.read_item.side_effect = Exception()

        db_service = DbService(self.db_options)
        db_service.container = mock_container

        with self.assertLogs(level="ERROR"):
            with self.assertRaises(Exception):
                db_service.get("test", "test_partition")

        db_service.container.read_item.assert_called_once()
