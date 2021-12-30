import pytest
from hive import cursor, prepare_database_cursor


class TestArraysOfArraysLateralViewDeclarationOrder():
    test_name = 'arrays_of_arrays_lateral_view_declaration_order'

    @pytest.fixture(scope='class')
    def class_cursor(self, cursor):
        return prepare_database_cursor(test_name=self.test_name, cursor=cursor)

    def test_sanity(self, class_cursor):
        # todo: at least one query
        assert True

