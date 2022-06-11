import json
import unittest

from src.db_service.DbService import DbService, DbOptions, Query
from tests.mocks.User import User
from unittest.mock import Mock

class QueryTests(unittest.TestCase):
    def setUp(self) -> None:
       self.dbOptions = DbOptions("test_endpoint", "test_key", "test_db_id", "test_container_id")

    def tearDown(self) -> None:
        self.dbOptions = None

    # Asserts data is queried from the database.
    def test_query_queries_data(self):
        userMocks = [
            User("test1", "testing").__dict__,
            User("test2", "testing").__dict__
        ]
        mock_container = Mock()
        mock_container.query_items.return_value = userMocks

        dbService = DbService(self.dbOptions)
        dbService.container = mock_container

        with self.assertLogs(level="INFO"):
            query = Query(
                queryStr="SELECT * FROM users"
            )
            result = dbService.query(query)

        j = json.loads(result)
        users = list[User]()
        for u in j:
            users.append(User(**u))

        self.assertEqual(2, len(users))

        # Test with where parameters.
        with self.assertLogs(level="INFO"):
            query = Query(
                queryStr="SELECT * FROM users WHERE id = @id",
                whereParams= {
                    "@id": "user::test"
                }
            )
            result = dbService.query(query)

        j = json.loads(result)
        users = list[User]()
        for u in j:
            users.append(User(**u))

        self.assertEqual(2, len(users))

    # Assert a TypeError is raised if the query object given is invalid.
    def test_query_raises_type_error(self):
        dbService = DbService(self.dbOptions)
        with self.assertLogs(level="ERROR"):
            with self.assertRaises(TypeError):
                dbService.query(None)
            
    # Asserts None is returned if no items are found.
    def test_query_returns_none(self):
        mock_container = Mock()
        mock_container.query_items.return_value = list()

        dbService = DbService(self.dbOptions)
        dbService.container = mock_container

        with self.assertLogs(level="WARNING"):
            query = Query(
                queryStr="SELECT * FROM users WHERE id = @id",
                whereParams= {
                    "@id": "user::test"
                }
            )
            result = dbService.query(query)

        self.assertIsNone(result)
        dbService.container.query_items.assert_called_once()

    # Assert a ValueError is raised if an empty query string is given.
    def test_query_raise_value_error_on_bad_query_string(self):
        with self.assertRaises(ValueError):
            query = Query(
                queryStr=" ",
            )
    
    # Assert Query class can build where parameters.
    def test_query_builds_where_params(self):
        query = Query(
            queryStr="SELECT * FROM users WHERE id = @id",
            whereParams= {
                "@id": "user::test"
            }
        )
        result = query.build_where_params()

        self.assertEqual(1, len(result))
