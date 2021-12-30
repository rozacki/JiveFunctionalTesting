import pytest
# cursor is needed as fixture
from hive import cursor, prepare_database_cursor


class TestConversions():
    test_name = 'conversions'

    @pytest.fixture(scope='class')
    def class_cursor(self, cursor):
        return prepare_database_cursor(test_name=self.test_name, cursor=cursor)

    # def test_sanity(self, class_cursor):
    #     # table names is in the mapping
    #     sql = f'SELECT count(*) FROM {self.test_name} WHERE array1_array2 IS NOT NULL'
    #
    #     class_cursor.execute(sql)
    #
    #     rows = class_cursor.fetchall()
    #
    #     assert rows[0][0] == 3

    def test_count_not_null_strings(self, class_cursor):
        column = 'string_column'
        sql = f'SELECT COUNT(*) FROM {self.test_name} WHERE {column} IS NOT NULL'
        class_cursor.execute(sql)
        rows = class_cursor.fetchone()
        assert 1 == rows[0]

    def test_count_not_null_ints(self, class_cursor):
        column = 'int_column'
        sql = f'SELECT COUNT(*) FROM {self.test_name} WHERE {column} IS NOT NULL'
        class_cursor.execute(sql)
        rows = class_cursor.fetchone()
        assert 1 == rows[0]

    def test_count_not_nul_dates(self, class_cursor):
        column = 'date_column'
        sql = f'SELECT COUNT(*) FROM {self.test_name} WHERE {column} IS NOT NULL'
        class_cursor.execute(sql)
        rows = class_cursor.fetchone()
        assert 1 == rows[0]

    def test_count_not_null_timestamps(self, class_cursor):
        column = 'time_column'
        sql = f'SELECT COUNT(*) FROM {self.test_name} WHERE {column} IS NOT NULL'
        class_cursor.execute(sql)
        rows = class_cursor.fetchone()
        assert 1 == rows[0]

    def test_count_not_null_booleans(self, class_cursor):
        column = 'bool_column'
        sql = f'SELECT COUNT(*) FROM {self.test_name} WHERE {column} IS NOT NULL'
        class_cursor.execute(sql)
        rows = class_cursor.fetchone()
        assert 1 == rows[0]