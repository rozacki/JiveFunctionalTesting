import pytest
from hive import cursor, prepare_database_cursor


class TestArraysOfArrays():
    test_name = 'arrays_of_arrays'

    @pytest.fixture(scope='class')
    def class_cursor(self, cursor):
        return prepare_database_cursor(test_name=self.test_name, cursor=cursor)

    def test_count(self, class_cursor):
        # table names is in the mapping
        sql = f'SELECT count(*) FROM {self.test_name} WHERE array1_array2 IS NOT NULL'

        class_cursor.execute(sql)

        all_rows = class_cursor.fetchall()

        assert all_rows[0][0] == 3

    def test_rows(self, class_cursor):
        sql = f'SELECT array1_array2 FROM {self.test_name} WHERE array1_array2 IS NOT NULL'
        class_cursor.execute(sql)
        all_rows = class_cursor.fetchall()

        assert 'a' == all_rows[0][0]
        assert 'b' == all_rows[1][0]
        assert 'c' == all_rows[2][0]

        assert True

    def test_nested_array_and_struct_count(self, class_cursor):
        sql = f'SELECT count(*) FROM {self.test_name} WHERE array1_array3_a IS NOT NULL'
        class_cursor.execute(sql)
        all_rows = class_cursor.fetchall()
        assert 3 == all_rows[0][0]

    def test_nested_array_and_struct_rows(self, class_cursor):
        sql = f"SELECT array1_array3_a,array1_array3_b FROM {self.test_name} WHERE array1_array3_a is not null and array1_array3_b IS NOT NULL"
        class_cursor.execute(sql)
        all_rows = class_cursor.fetchall()

        assert ('a1', 'b1',) == all_rows[0]
        assert ('a2', 'b2',) == all_rows[1]
        assert ('a3', 'b3',) == all_rows[2]

    def test_array_single_element(self, class_cursor):
        sql = f"SELECT array1_array4_array4_index FROM {self.test_name} where  array1_array4_array4_index is not null"
        class_cursor.execute(sql)
        all_rows = class_cursor.fetchall()

        assert 1 == len(all_rows)

    def test_nested_array_and_index_rows(self, class_cursor):
        sql = f'SELECT array1_array4_array4_index FROM  {self.test_name} where  array1_array4_array4_index is not null'
        class_cursor.execute(sql)
        all_rows = class_cursor.fetchall()

        assert 1 == all_rows[0][0]