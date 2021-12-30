import pytest
# cursor is needed as fixture
from hive import cursor, prepare_database_cursor


class TestArraysOfArraysLateralViewDeclarationOrder():
    test_name = 'arrays_of_arrays_lateral_view_declaration_order'

    @pytest.fixture(scope='class')
    def class_cursor(self, cursor):
        return prepare_database_cursor(test_name=self.test_name, cursor=cursor)

    def test_sanity(self, class_cursor):
        # table names is in the mapping
        sql = f'SELECT count(*) FROM {self.test_name} WHERE array1_array2 IS NOT NULL'

        class_cursor.execute(sql)

        rows = class_cursor.fetchall()

        assert rows[0][0] == 3


