import json
import unittest

from src.db_service.DbService import DbService, DbOptions
from tests.mocks.User import User
from unittest.mock import Mock

class UpsertTests(unittest.TestCase):
    
    def setUp(self) -> None:
       self.dbOptions = DbOptions("test_endpoint", "test_key", "test_db_id", "test_container_id")

    def tearDown(self) -> None:
        self.dbOptions = None

    # Asserts an item can be upserted.
    def test_upsert_upserts_item(self):
        user_mocks = User("test", "testing")
        mock_container = Mock()
        mock_container.upsert_item.return_value = user_mocks.__dict__

        dbService = DbService(self.dbOptions)
        dbService.container = mock_container

        with self.assertLogs(level="INFO"):
            result = dbService.upsert(user_mocks.__dict__)

        j = json.loads(result)
        user = User(**j)

        self.assertIsInstance(user, User)
        self.assertEqual("test", user.user)
        self.assertEqual("testing", user.password)

    # Asserts a TypeError is raised if the parameter given is invalid.
    def test_upsert_raises_type_error(self):
        dbService = DbService(self.dbOptions)

        with self.assertLogs(level="ERROR"):
            with self.assertRaises(TypeError):
                dbService.upsert(None)

    # Asserts an exception is raised if an unexpected error occurs.
    def test_upsert_raises_exception(self):
        user_mocks = User("test", "testing")
        mock_container = Mock()
        mock_container.upsert_item.side_effect = Exception

        dbService = DbService(self.dbOptions)
        dbService.container = mock_container

        with self.assertLogs(level="ERROR"):
            with self.assertRaises(Exception):
                dbService.upsert(user_mocks.__dict__)

        dbService.container.upsert_item.assert_called_once()