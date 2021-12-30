import pytest
from hive import cursor, prepare_database_cursor


class TestArrays():
    test_name = 'arrays'

    @pytest.fixture(scope='class')
    def class_cursor(self, cursor):
        return prepare_database_cursor(test_name=self.test_name, cursor=cursor)

    def test_explode(self, class_cursor):
        # table names is in the mapping
        sql = f'SELECT count(*) FROM {self.test_name} WHERE array3 IS NOT NULL'

        class_cursor.execute(sql)
        rows = class_cursor.fetchall()
        assert rows[0][0] == 9