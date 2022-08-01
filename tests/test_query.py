import json
import unittest

from src.db_service.DbService import DbService, DbOptions, Query
from tests.mocks.User import User
from unittest.mock import Mock

class QueryTests(unittest.TestCase):
    def setUp(self) -> None:
       self.db_options = DbOptions("test_endpoint", "test_key", "test_db_id", "test_container_id")

    def tearDown(self) -> None:
        self.db_options = None

    # Asserts data is queried from the database.
    def test_query_queries_data(self):
        user_mocks = [
            User("test1", "testing").__dict__,
            User("test2", "testing").__dict__
        ]
        mock_container = Mock()
        mock_container.query_items.return_value = user_mocks

        db_service = DbService(self.db_options)
        db_service.container = mock_container

        with self.assertLogs(level="INFO"):
            query = Query(
                query_str="SELECT * FROM users"
            )
            result = db_service.query(query)

        j = json.loads(result)
        users = list[User]()
        for u in j:
            users.append(User(**u))

        self.assertEqual(2, len(users))

        # Test with where parameters.
        with self.assertLogs(level="INFO"):
            query = Query(
                query_str="SELECT * FROM users WHERE id = @id",
                where_params= {
                    "@id": "user::test"
                }
            )
            result = db_service.query(query)

        j = json.loads(result)
        users = list[User]()
        for u in j:
            users.append(User(**u))

        self.assertEqual(2, len(users))

    # Assert a TypeError is raised if the query object given is invalid.
    def test_query_raises_type_error(self):
        db_service = DbService(self.db_options)
        with self.assertLogs(level="ERROR"):
            with self.assertRaises(TypeError):
                db_service.query(None)
            
    # Asserts None is returned if no items are found.
    def test_query_returns_none(self):
        mock_container = Mock()
        mock_container.query_items.return_value = list()

        db_service = DbService(self.db_options)
        db_service.container = mock_container

        with self.assertLogs(level="WARNING"):
            query = Query(
                query_str="SELECT * FROM users WHERE id = @id",
                where_params= {
                    "@id": "user::test"
                }
            )
            result = db_service.query(query)

        self.assertIsNone(result)
        db_service.container.query_items.assert_called_once()

    # Assert a ValueError is raised if an empty query string is given.
    def test_query_raise_value_error_on_bad_query_string(self):
        with self.assertRaises(ValueError):
            Query(
                query_str=" ",
            )
    
    # Assert Query class can build where parameters.
    def test_query_builds_where_params(self):
        query = Query(
            query_str="SELECT * FROM users WHERE id = @id",
            where_params= {
                "@id": "user::test"
            }
        )
        result = query.build_where_params()

        self.assertEqual(1, len(result))
