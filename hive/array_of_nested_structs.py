import pytest
from hive import cursor, prepare_database_cursor


class TestArrayOfNestedStructs():
    test_name = 'array_of_nested_structs'

    @pytest.fixture(scope='class')
    def class_cursor(self, cursor):
        return prepare_database_cursor(test_name=self.test_name, cursor=cursor)

    def test_explode_array(self, class_cursor):
        sql = "SELECT * FROM " + self.test_name;

        class_cursor.execute(sql)
        rows = class_cursor.fetchall()

        assert len(rows) == 4

    def test_single_element(self, class_cursor):
        sql = 'SELECT parentId,structure_id1,structure_value1,structure_id2,structure_value2 FROM ' + self.test_name

        class_cursor.execute(sql)
        rows = class_cursor.fetchall()

        assert (1, 'ID3', 100, None, None,) == rows[0]
        assert (1, None, None, 'ID5', 200,) == rows[1]
        assert (2, 'ID3', 1000, None, None,) == rows[2]
        assert (2, None, None, 'ID5', 2000,) == rows[3]

