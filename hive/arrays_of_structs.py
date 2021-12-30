import pytest
from hive import prepare_database_cursor


class TestArraysOfStructs():
    # todo : get file name and remove py
    test_name = 'arrays_of_structs'

    @pytest.fixture(scope='class')
    def class_cursor(self, cursor):
        return prepare_database_cursor(test_name=self.test_name, cursor=cursor)

    def test_expode_test(self, class_cursor):
        # table names is in the mapping
        sql = f'SELECT count(*) FROM {self.test_name} WHERE array1_array2 IS NOT NULL'
        class_cursor.execute(sql)
        rows = class_cursor.fetchall()

        assert len(rows) == 3

