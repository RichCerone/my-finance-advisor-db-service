import json
import unittest

from db_service.DbService import DbService, DbOptions, CosmosHttpResponseError
from mocks.User import User
from unittest.mock import Mock

class GetTests(unittest.TestCase):

    def setUp(self) -> None:
       self.dbOptions = DbOptions("test_endpoint", "test_key", "test_db_id", "test_container_id")

    def tearDown(self) -> None:
        self.dbOptions = None

    # Asserts an item can be retrieved.
    def test_get_gets_item(self):
        userMock = User("test", "testing")
        mock_container = Mock()
        mock_container.read_item.return_value = userMock.__dict__

        dbService = DbService(self.dbOptions)
        dbService.container = mock_container

        with self.assertLogs(level="INFO"):
            result = dbService.get("user::test", "user")

        j = json.loads(result)
        user = User(**j)

        self.assertIsInstance(user, User)
        self.assertEqual("test", user.user)
        self.assertEqual("testing", user.password)

    # Asserts a ValueError is raised if the parameters are invalid.
    def test_get_raises_value_error(self):
        dbService = DbService(self.dbOptions)

        with self.assertLogs(level="ERROR"):
            with self.assertRaises(ValueError):
                dbService.get(" ", "test_partition")

        with self.assertLogs(level="ERROR"):
            with self.assertRaises(ValueError):
                dbService.get(None, "test_partition")
        
        with self.assertLogs(level="ERROR"):
            with self.assertRaises(ValueError):
                dbService.get("test", " ")

        with self.assertLogs(level="ERROR"):
            with self.assertRaises(ValueError):
                dbService.get("test", None)

    # Asserts None is returned if no item is found.
    def test_get_cannot_find_item(self):
        mock_container = Mock()
        mock_container.read_item.side_effect = CosmosHttpResponseError()

        dbService = DbService(self.dbOptions)
        dbService.container = mock_container

        with self.assertLogs(level="WARNING"):
            result = dbService.get("test", "test_partition")

        self.assertEqual(None, result)
        dbService.container.read_item.assert_called_once()

    # Asserts an Exception is raised if an unexpected error occurs.
    def test_get_raises_exception(self):
        mock_container = Mock()
        mock_container.read_item.side_effect = Exception()

        dbService = DbService(self.dbOptions)
        dbService.container = mock_container

        with self.assertLogs(level="ERROR"):
            with self.assertRaises(Exception):
                dbService.get("test", "test_partition")

        dbService.container.read_item.assert_called_once()
